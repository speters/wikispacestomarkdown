#!/usr/bin/python3
# coding: utf-8
# vim: set fileencoding=utf-8 :

# see http://helpcenter.wikispaces.com/customer/portal/articles/1964502-api-customizations

import logging

from suds.client import Client
from suds.xsd.doctor import ImportDoctor, Import
from suds.bindings.binding import Binding

import datetime
import dataset
from os import urandom
import sys
import hashlib
import time

import csv
import requests
from urllib.parse import urlparse, parse_qs


from wstomdconverter import WikispacesToMarkdownConverter
from reportlab.platypus import tableofcontents
from scrapy.selector import Selector
from pyxb.bundles.wssplat.raw.wsa import From

logging.getLogger("requests").setLevel(logging.WARNING)

_lastreply = ''
def replyfilter(r):
    lastreply = r
    return r

Binding.replyfilter = (lambda s, r: replyfilter(r))

# from slugify import slugify
def slugify(s):
    return s.replace('/', '_').replace('\\', '_')

loginfo = logging.info

def now():
    return int(datetime.datetime.utcnow().timestamp())

class WikiSpaces(object):
    urlformat = 'http://www.wikispaces.com/{}/api'
    imp = Import('http://schemas.xmlsoap.org/soap/encoding/')
    (imp.filter.add(urlformat.format(i)) for i in ('site', 'space', 'user', 'page', 'message'))
    doctor = ImportDoctor(imp)
    cachetime = 1 * 3600
    # wikispaces "stamdard" timestamp seems to be in PST (Pacific Standard Time / UTC - 8h), whileas we live in CET / UTC +1h
    # we could stay in this TZ, but then git logs also show this TZ, which is annoying
    # to adjust the timestamp to the actual displayed time of our TZ (CET), it's neccessary to substract 9h:
    timeoffset = 8 * 3600
    db = None

    @staticmethod
    def dbconnect(dbname):
        WikiSpaces.db = dataset.connect(dbname)
        loginfo('Connected to database {}'.format(dbname))

    # see https://github.com/migrateup/python-observer-pattern/blob/master/observer3.py
    def init_events(self, events = None):
        # maps event names to subscribers
        # str -> dict
        if events is None:
            events = WikiSpaces.events
        self.events = { event : dict() for event in events }

    def get_subscribers(self, event):
        return self.events[event]

    def register(self, event, who, callback = None):
        if callback == None:
            callback = getattr(who, 'update')
        self.get_subscribers(event)[who] = callback

    def unregister(self, event, who):
        del self.get_subscribers(event)[who]

    def dispatch(self, event, message):
        for subscriber, callback in self.get_subscribers(event).items():
            callback(message)

    @staticmethod
    def dict(struct):
        try:
            return dict((field, getattr(struct, field)) for field, _ in struct._fields_)
        except AttributeError:
            return dict((field, getattr(struct, field)) for field, _ in struct)

    @staticmethod
    def getspaceid(spacename):
        spacestruct = WikiSpaces.db['space'].find_one(name = spacename)
        return int(spacestruct['id'])

    @staticmethod
    def getspacename(spaceid):
        spacestruct = WikiSpaces.db['space'].find_one(id = spaceid)
        return spacestruct['name']

    @staticmethod
    def gettimestampfromwstime(s, f = "%Y-%m-%d %H:%M:%S", o = None):
        if o is None:
            o = WikiSpaces.timeoffset
        else:
            o = int(o)
        return int(time.mktime(time.strptime(s, f)) + o)

class Session(object):
    def __init__(self, session):
        self.session = session
        self.sessiontime = datetime.datetime.utcnow()

    def getAge(self):
        return (datetime.datetime.utcnow() - self.sessiontime)

    def __str__(self):
        return '{}:{}'.format(self.session, self.sessiontime)

    def __repr__(self):
        return self.session

class Site(WikiSpaces):
    url = WikiSpaces.urlformat.format('site') + '?wsdl'

    def __init__(self, dbname = 'sqlite:///wikispaces.sqlite'):
        self.siteApi = Client(Site.url)
        if WikiSpaces.db is None:
            WikiSpaces.dbconnect(dbname)
        self.db = WikiSpaces.db
        self.init_events(['create', 'delete'])

    def login(self, username, password):
        self.session = Session(self.siteApi.service.login(username, password))
        logging.debug('Logged in as user {}'.format(username))
        self.dispatch('create', ('session', self.session))
        return self.session

    def logout(self):
        self.dispatch('delete', ('session', self.session))
        self.session = None

# TODO: Split Space into Space, Member
class Space(WikiSpaces):
    url = WikiSpaces.urlformat.format('space') + '?wsdl'

    def __init__(self, spacename, session = None):
        self.spaceApi = Client(Space.url, doctor = WikiSpaces.doctor)
        self.session = session
        self.spacename = spacename
        self.spaceid = None
        self.init_events(['create', 'update', 'delete'])  # A bit misleading, because these are events regarding members
        self.csvmemberlist = None
        self.memberlist = {}
        self.memberlist_time = 0

        try:
            self.dbtable_space = WikiSpaces.db.load_table('space')
        except:  # sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE space (
                    id INTEGER,
                    name VARCHAR NOT NULL,
                    text VARCHAR,
                    description VARCHAR,
                    status VARCHAR,
                    image_type VARCHAR,
                    background_color VARCHAR,
                    highlight_color VARCHAR,
                    text_color VARCHAR,
                    link_color VARCHAR,
                    subscription_type VARCHAR,
                    subscription_level VARCHAR,
                    subscription_end_date INTEGER,
                    is_crawled BOOLEAN,
                    license VARCHAR,
                    discussions VARCHAR,
                    date_created INTEGER,
                    date_updated INTEGER,
                    user_created INTEGER,
                    user_updated INTEGER,
                    page_count INTEGER,
                    view_group VARCHAR,
                    edit_group VARCHAR,
                    create_group VARCHAR,
                    message_edit_group VARCHAR,
                    edits INTEGER,
                    cachetime INTEGER,
                    cachetime_members INTEGER,
                    date_exported_page INTEGER DEFAULT 0,
                    date_exported_message INTEGER DEFAULT 0,
                    date_exported_file INTEGER DEFAULT 0,
                    date_changes_check INTEGER DEFAULT 0,
                    PRIMARY KEY(id));''')
            self.dbtable_space = WikiSpaces.db.load_table('space')
            self.dbtable_space.create_index(['id'])
            logging.info('Created database table space')

        self.get()

        try:
            self.dbtable_members = WikiSpaces.db.load_table('members')
        except:  # sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE members (
                    id INTEGER,
                    userId INTEGER,
                    username VARCHAR NOT NULL,
                    spaceid INTEGER NOT NULL,
                    type VARCHAR,
                    joined INTEGER DEFAULT 0,
                    deleted INTEGER DEFAULT 0,
                    PRIMARY KEY(id));''')
            self.dbtable_members = WikiSpaces.db.load_table('members')
            self.dbtable_members.create_index(['username', 'spaceid'])
            logging.info('Created database table members')


    def get(self):
        self.spacestruct = self.dbtable_space.find_one(name = self.spacename)
        if self.spacestruct is None:
            if (self.session is None):
                logging.error('No space {} in db. Valid session needed for space.getlive()'.format(self.spacename))
                return None
            else:
                self.getlive()
        else:
            if ((now() - self.spacestruct['cachetime']) > WikiSpaces.cachetime):
                if self.session is None:
                    logging.info('Max cachetime reached, but no valid session for space.getlive(). Using old data...')
                else:
                    self.getlive()
            self.spacestruct = dict(self.spacestruct)
            self.lastupdate = self.spacestruct['cachetime']
            self.spaceid = self.spacestruct['id']
        return self.spacestruct

    def getlive(self):
        space = self.spaceApi.service.getSpace(self.session.session, self.spacename)
        self.spacestruct = WikiSpaces.dict(space)
        self.lastupdate = now()
        self.spacestruct['cachetime'] = self.lastupdate
        self.spacestruct['cachetime_members'] = self.memberlist_time
        self.dbtable_space.upsert(self.spacestruct, keys = ['id'])  # ,ensure=True)
        self.spaceid = self.spacestruct['id']
        loginfo('Space.getlive()@{}'.format(self.spaceid))
        return self.spacestruct

    def listmembers(self):
        if ((not (self.session is None)) and (len(self.memberlist) == 0) or ((now() - self.memberlist_time) > WikiSpaces.cachetime)):
            return self.listmemberslive()
        else:
            s = self.dbtable_members.find(spaceid = self.spacestruct['id'])
            if not s is None:
                self.memberlist = dict((m['username'], dict(m)) for m in s)
                self.memberlist_time = self.spacestruct['cachetime_members']
            else:
                self.memberlist = {}
                self.memberlist_time = 0
            return self.memberlist

    def listmemberscsvlive(self):
        r = requests.get('https://openv.wikispaces.com/wiki/members', params = {'utable': 'WikiTableMemberList', 'ut_csv': 1})
        self.csvmemberlist = {}
        for row in csv.reader(r.text.split('\n'), delimiter = ',', quotechar = '"'):
            if len(row) > 1:
                if row[2] != 'Type':
                    joined = WikiSpaces.gettimestampfromwstime(row[1])
                    self.csvmemberlist[row[0]] = {'joined': joined, 'type': row[2]}

    def getmemberinfofromcsv(self, username):
        if self.csvmemberlist is None:
            self.listmemberscsvlive()
        try:
            return self.csvmemberlist[username]
        except KeyError:
            return None

    def getMember(self, username):
        member = self.dbtable_members.find_one(username=username)
        if member is None:
            self.listmemberslive()
        member = self.dbtable_members.find_one(username=username)
        return dict(member)

    def listmemberslive(self):
        members = self.spaceApi.service.listMembers(self.session.session, self.spacestruct['id'])
        l = {}
        if len(self.memberlist) == 0:
            s = self.dbtable_members.find(spaceid = self.spacestruct['id'])
            if not s is None:
                self.memberlist = dict((m['username'], dict(m)) for m in s)
        delmembers = self.memberlist
        self.memberlist_time = now()
        for m in members:
            m = WikiSpaces.dict(m)
            m['spaceid'] = self.spacestruct['id']

            if not m['username'] in self.memberlist:
                m['joined'] = self.getmemberinfofromcsv(m['username'])['joined']
                self.dbtable_members.insert(m)  # , ensure=True) # SOAP API does not deliver userId for all users, so use username
                self.dispatch('create', ('member', m))
                loginfo('New member {} @{}'.format(m['username'], m['spaceid']))
            else:
                if self.memberlist[m['username']]['deleted'] != 0:
                    m['deleted'] = 0
                    self.dbtable_members.update(m, ['username'])
                    self.dispatch('update', ('member', m))
                    loginfo('Re-joined member {} @{}'.format(m['username'], m['spaceid']))
                del(delmembers[m['username']])
            l[m['username']] = m

        for d in delmembers:
            #self.db.query('''UPDATE members SET deleted={:d} WHERE username='{}' AND deleted=0'''.format(self.memberlist_time, d)) # SOAP API does not deliver userId for all users, so use username
            self.db.query('''DELETE FROM members WHERE username='{}' '''.format(d)) # SOAP API does not deliver userId for all users, so use username
            self.dispatch('delete', ('member', d))
            loginfo('Deleted member {} @{}'.format(d , self.spacestruct['id']))

        self.memberlist = l
        self.dbtable_space.update(dict(id = self.spacestruct['id'], cachetime_members = self.memberlist_time), ['id'])
        self.spacestruct['cachetime_members'] = self.memberlist_time
        loginfo('Space.listmemberslive()@{}'.format(self.spaceid))
        return self.memberlist

    def __str__(self):
        return str(self.spacestruct)

class Pages(WikiSpaces):
    url = WikiSpaces.urlformat.format('page') + '?wsdl'

    def __init__(self, space, session = None):
        if type(space) == Space:
            self.spaceid = space.spacestruct['id']
            # self.spacename = space.spacestruct['name']
        elif type(space) == int:
            self.spaceid = space
            # self.spacename = WikiSpaces.getspacename(self.spaceid)
        else:
            self.spacename = space
            self.spaceid = WikiSpaces.getspaceid(self.spacename)

        self.pageApi = Client(Pages.url, doctor = WikiSpaces.doctor)
        self.session = session
        self.init_events(['create', 'update', 'delete'])

        try:
            self.dbtable_page = WikiSpaces.db.load_table('page')
        except:  # sqlalchemy.exc.NoSuchTableError as e:
            # TODO: check if versionId could be used as PRIMARY KEY (versionId looks unique in _our_ data)
            WikiSpaces.db.query('''
                CREATE TABLE page (
                    id INTEGER,
                    pageId INTEGER NOT NULL,
                    versionId INTEGER NOT NULL,
                    name VARCHAR NOT NULL,
                    spaceId INTEGER NOT NULL,
                    latest_version INTEGER,
                    versions INTEGER,
                    is_read_only BOOLEAN,
                    view_group VARCHAR,
                    edit_group VARCHAR,
                    comment VARCHAR DEFAULT '',
                    content TEXT NOT NULL,
                    html TEXT,
                    date_created INTEGER,
                    user_created INTEGER,
                    user_created_username VARCHAR,
                    deleted INTEGER DEFAULT 0,
                    cachetime INTEGER,
                    PRIMARY KEY(id));''')
            self.dbtable_page = WikiSpaces.db.load_table('page')
            self.dbtable_page.create_index(['id'])
            logging.info('Created database table page')
        self.pagelist = []
        self.lastupdate = 0

    def listPages(self, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid

        if (not(self.session is None)) and ((len(self.pagelist) == 0) or (now() - self.lastupdate) > WikiSpaces.cachetime):
            return self.listPageslive(spaceid)
        else:
            return self.pagelist

    def listPageslive(self, spaceid = None):
        # gets a condensed list of pages (eg. no text content), so don't save into DB
        if spaceid is None:
            spaceid = self.spaceid

        pages = self.pageApi.service.listPages(self.session.session, spaceid)
        cachetime = now()
        l = []
        for page in pages:
            page = WikiSpaces.dict(page)
            page['pageId'] = page['id']
            del(page['id'])
            page['cachetime'] = cachetime
            l.append(page)
        self.pagelist = l
        loginfo('Pages.listPageslive()@{}'.format(spaceid))
        return self.pagelist

    def getPage(self, pagename, pageversion = None, pageid = None, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid
        if pageversion is None:
            if (not(pageid is None)) and (type(pageid) == int):
                page = self.dbtable_page.find_one(pageId = pageid, spaceId = spaceid, order_by = '-date_created')
            else:
                page = self.dbtable_page.find_one(name = pagename, spaceId = spaceid, order_by = '-date_created')
        else:
            page = self.dbtable_page.find_one(spaceId = spaceid, versionId = pageversion)
        if (not(self.session is None)) and ((page is None)):  # or ((now() - page['cachetime']) > WikiSpaces.cachetime)):
            return self.getPagelive(pagename, pageversion, spaceid)
        else:
            return dict(page)

    def getPagelive(self, pagename, pageversion = None, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid

        if pageversion is None:
            page = self.pageApi.service.getPage(self.session.session, spaceid, pagename)
        else:
            page = self.pageApi.service.getPageWithVersion(self.session.session, spaceid, pagename, pageversion)

        cachetime = now()
        page = WikiSpaces.dict(page)
        page['pageId'] = page['id']
        del(page['id'])
        page['cachetime'] = cachetime
        loginfo('Pages.getPagelive(pagename="{}", pageversion="{}")@{}'.format(pagename, str(pageversion), spaceid))
        oldver = self.dbtable_page.find_one(pageId = page['pageId'], spaceId = spaceid)
        self.dbtable_page.insert(page) # ,ensure=True)

        if oldver is None:
            self.dispatch('create', ('page', page))
        else:
            self.dispatch('update', ('page', page))

        return page

    def listPageVersionslive(self, pagename, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid

        pagelist = self.pageApi.service.listPageVersions(self.session.session, spaceid, pagename)

        cachetime = now()
        p = []
        for page in pagelist:
            page = WikiSpaces.dict(page)
            page['pageId'] = page['id']
            del(page['id'])
            page['cachetime'] = cachetime
            p.append(page)
        loginfo('Pages.listPageVersionslive(pagename="{}")@{}'.format(pagename, spaceid))
        return p

    def getPageVersionslive(self, pagename, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid

        pagelist = self.listPageVersionslive(pagename, spaceid)
        cachetime = now()

        s = self.dbtable_page.find(name = pagename, spaceId = spaceid)
        v = {}
        if not s is None:
            v = dict((m['versionId'], dict(m)) for m in s)

        pageversions = []
        for p in pagelist:
            page = self.getPage(p['name'], p['versionId'], p['spaceId'])
            pageversions.append(page)
            try:
                del(v[page['versionId']])
            except KeyError:
                pass

        for versionid, page in v.items():
            self.db.query('''UPDATE page SET deleted={:d} WHERE versionID={:d} AND pageId={:d} AND spaceId={:d} AND deleted=0'''
                          .format(cachetime, versionid, p['pageId'], p['spaceId']))
            self.dispatch('delete', ('page', page))
            loginfo('Deleted Page {} (Version="{}")@{}'.format(page['name'], versionid, spaceid))

        return pageversions

    def getPageslive(self, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid

        s = self.dbtable_page.distinct('pageId', spaceId = spaceid)
        cachetime = now()
        l = {}
        if not s is None:
            l = dict((p['pageId'], dict(p)) for p in s)

        pagelist = self.listPageslive(spaceid)
        for page in pagelist:
            self.getPageVersionslive(page['name'], spaceid)
            try:
                del(l[page['pageId']])
            except KeyError:
                pass

        for pageid, p in l.items():
            self.db.query('''UPDATE page SET deleted={:d} WHERE pageId={:d} AND spaceId={:d} AND deleted=0'''
                          .format(cachetime, pageid, spaceid))
            self.dispatch('delete', ('page', page))
            loginfo('Deleted Page {} @{}'.format(p['name'], spaceid))

        return pagelist

    # TODO: Check for renamed pages

class Messages(WikiSpaces):
    url = WikiSpaces.urlformat.format('message') + '?wsdl'

    def __init__(self, session = None):
        self.messageApi = Client(Messages.url, doctor = WikiSpaces.doctor)
        self.session = session
        self.init_events(['create', 'update', 'delete'])

        try:
            self.dbtable_message = WikiSpaces.db.load_table('message')
        except:  # sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE message (
                    id INTEGER,
                    subject VARCHAR,
                    body TEXT NOT NULL,
                    html TEXT,
                    page_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    responses INTEGER,
                    latest_response_id INTEGER,
                    date_response INTEGER,
                    user_created INTEGER,
                    user_created_username VARCHAR,
                    date_created INTEGER,
                    deleted INTEGER DEFAULT 0,
                    cachetime INTEGER,
                    PRIMARY KEY(id));''')
            self.dbtable_message = WikiSpaces.db.load_table('message')
            self.dbtable_message.create_index(['id'])
            logging.info('Created database table message')
        self.topiclist = {}
        self.lastupdate = 0

    def listTopics(self, pageid):
        try:
            topic = self.topiclist[pageid]
        except KeyError:
            topic = None
        if (not(self.session is None)) and ((topic is None) or (now() - self.lastupdate) > WikiSpaces.cachetime):
            return self.listTopicslive(pageid)
        else:
            return self.topiclist[pageid]

    def listTopicslive(self, pageid):
        topics = self.messageApi.service.listTopics(self.session.session, pageid)
        oldtopics = self.dbtable_message.distinct('topic_id', page_id = pageid)
        oldtopics = dict((m['topic_id'], dict(m)) for m in oldtopics)
        cachetime = now()
        l = []
        for topic in topics:
            topic = WikiSpaces.dict(topic)
            topic['cachetime'] = cachetime
            l.append(topic)
            if not (topic['topic_id'] in oldtopics):
                self.dispatch('create', ('topic', topic))
            else:
                del(oldtopics[topic['topic_id']])
            self.dbtable_message.upsert(topic, keys = ['id'])  # ,ensure=True)
        self.topiclist[pageid] = l

        for t in oldtopics:
            t['deleted'] = cachetime
            self.db.query('''UPDATE message SET deleted={:d} WHERE topic_id={:d} AND deleted=0'''
                          .format(cachetime, t['topic_id']))
            self.dispatch('delete', ('topic', t))

        loginfo('Messages.listTopicslive(pageid="{}")'.format(pageid))
        return self.topiclist[pageid]

    def listMessagesInTopic(self, topicid):
        topic = self.dbtable_message.find_one(topic_id = topicid, order_by = '-cachetime')
        topic = dict(topic) if not topic is None else None
        latestreponse = None
        if not (topic is None):
            latestresponse = self.dbtable_message.find_one(id = topic['latest_response_id'])
        if (not(self.session is None)) and ((topic is None) or (latestresponse is None)):
            return self.listMessagesInTopiclive(topicid)
        else:
            return topic

    def listMessagesInTopiclive(self, topicid):
        try:
            messages = self.messageApi.service.listMessagesInTopic(self.session.session, topicid)
        except:  # xml.sax._exceptions.SAXParseException:
            if len(_lastreply) == 0:
                logging.error('Empty reply for Messages.listMessagesInTopiclive(topicid="{}")'.format(topicid))
            else:
                logging.error('Could not get topic in Messages.listMessagesInTopiclive(topicid="{}")'.format(topicid))
            # with open('reply_topic{}'.format(topicid), 'w') as file:
            #   file.write(_lastreply)
            return None
        oldmessages = self.dbtable_message.find(topic_id = topicid)
        oldmessages = dict((m['id'], dict(m)) for m in oldmessages)
        cachetime = now()
        for message in messages:
            message = WikiSpaces.dict(message)
            message['cachetime'] = cachetime
            if not (message['id'] in oldmessages):
                self.dbtable_message.insert(message)  # ,ensure=True)
                self.dispatch('create', ('message', message))
            else:
                if (message['subject'] != oldmessages[message['id']]['subject']) or (message['body'] != oldmessages[message['id']]['body']):
                    self.dispatch('update', ('message', message))
                self.dbtable_message.update(message, keys = ['id'])  # ,ensure=True)
                del(oldmessages[message['id']])

        for t in oldmessages:
            t['deleted'] = cachetime
            self.db.query('UPDATE message SET deleted={:d} WHERE id={:d} AND deleted=0'.format(cachetime, t['id']))
            self.dispatch('delete', ('message', t))

        loginfo('Messages.listMessagesInTopiclive(topicid="{}")'.format(topicid))
        return messages

    def getAllMessagesInPage(self, pageid):
        l = self.listTopics(pageid)
        a = []
        for t in l:
            m = self.listMessagesInTopic(t['topic_id'])
            a.append(m)
        return a

class Users(WikiSpaces):
    url = WikiSpaces.urlformat.format('user') + '?wsdl'

    def __init__(self, session = None):
        self.userApi = Client(Users.url, doctor = WikiSpaces.doctor)
        self.session = session
        self.init_events(['create', 'update'])

        try:
            self.dbtable_user = WikiSpaces.db.load_table('user')
        except:  # sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE user (
                    id INTEGER,
                    username VARCHAR NOT NULL,
                    posts INTEGER,
                    edits INTEGER,
                    date_created INTEGER,
                    date_updated INTEGER,
                    user_created INTEGER,
                    user_updated INTEGER,
                    auth_source_id INTEGER,
                    auth_external_id VARCHAR,
                    cachetime INTEGER,
                    PRIMARY KEY(id));''')
            self.dbtable_user = WikiSpaces.db.load_table('user')
            self.dbtable_user.create_index(['id'])
            logging.info('Created database table user')
        self.lastupdate = 0

    def getUser(self, username):
        user = self.dbtable_user.find_one(username = username)
        if (not(self.session is None)) and ((user is None)):  # or (now() - self.lastupdate) > WikiSpaces.cachetime):
            return self.getUserlive(username)
        else:
            return dict(user)

    def getUserlive(self, username):
        user = self.userApi.service.getUser(self.session.session, username)
        user = WikiSpaces.dict(user)
        user['cachetime'] = now()
        olduser = self.dbtable_user.find_one(id = user['id'])
        if olduser == None:
            self.dbtable_user.insert(user)  # ,ensure=True)
            self.dispatch('create', ('user', user))
        else:
            self.dbtable_user.update(user, keys = ['id'])  # ,ensure=True)
            # self.dispatch('update', ('user', user))
        loginfo('Users.getUserlive(username="{}")'.format(username))
        return user

    def getUserById(self, userid):
        userid = int(userid)
        user = self.dbtable_user.find_one(id = userid)
        if (not(self.session is None)) and ((user is None)):  # or (now() - self.lastupdate) > WikiSpaces.cachetime):
            return self.getUserByIdlive(userid)
        else:
            return dict(user)

    def getUserByIdlive(self, userid):
        userid = int(userid)
        user = self.userApi.service.getUserById(self.session.session, userid)
        user = WikiSpaces.dict(user)
        user['cachetime'] = now()
        olduser = self.dbtable_user.find_one(username = user['username'])
        if olduser == None:
            self.dbtable_user.insert(user)  # ,ensure=True)
            self.dispatch('create', ('user', user))
        else:
            self.dbtable_user.update(user, keys = ['id'])  # ,ensure=True)
            # self.dispatch('update', ('user', user))
        loginfo('Users.getUserByIdlive(userid="{}")'.format(userid))
        return user

class Files(WikiSpaces):
    def __init__(self, space):
        if type(space) == Space:
            self.spaceid = space.spacestruct['id']
            # self.spacename = space.spacestruct['name']
        elif type(space) == int:
            self.spaceid = space
        else:
            self.spaceid = WikiSpaces.getspaceid(space)

        self.spacename = WikiSpaces.getspacename(self.spaceid)
        self.init_events(['create', 'update', 'delete'])

        try:
            self.dbtable_file = WikiSpaces.db.load_table('file')
        except:  # sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE file (
                    id INTEGER NOT NULL,
                    name VARCHAR NOT NULL,
                    size INTEGER DEFAULT 0,
                    type VARCHAR DEFAULT 'application/octet-stream',
                    contents BLOB,
                    spaceid INTEGER NOT NULL,
                    username VARCHAR NOT NULL,
                    date_created INTEGER DEFAULT 0,
                    deleted INTEGER DEFAULT 0,
                    cachetime INTEGER,
                    PRIMARY KEY(id));''')
            self.dbtable_file = WikiSpaces.db.load_table('file')
            self.dbtable_file.create_index(['id'])
            logging.info('Created database table file')

            self.filelist = {}

    def getAllFiles(self):
        filelist = self.getFilelistlive()
        for filename in filelist:
            fileinfo = self.dbtable_file.find_one(name = filename, order_by = '-date_created')
            if not fileinfo is None:
                timediff = (filelist[filename]['date_created'] - fileinfo['date_created'])
                if not timediff in [-3600, 0, 3600]:  # TODO: check why/when diff of 3600 occurred
                    loginfo('File {} has as changed, get versions from history page'.format(filename))
                    self.getFileHistorylive(filename)
            else:
                loginfo('File {} not in db, get versions from history page'.format(filename))
                self.getFileHistorylive(filename)

    def getFilelistlive(self):
        # for files, and their versions, we need to scrape the info from web pages, as there is no dedicated API
        r = requests.get("https://{}.wikispaces.com/space/content".format(self.spacename), params = {'utable': 'WikiTablePageList', 'ut_csv': 1})

        oldfiles = self.dbtable_file.distinct('name', spaceid = self.spaceid)
        oldfiles = dict((f['name'], dict(f)) for f in oldfiles)
        cachetime = now()

        self.filelist = {}
        for row in csv.reader(r.text.split('\n'), delimiter = ',', quotechar = '"'):
            # Type,Name,Size,Status,Last Edited By,Date Last Edited (America/Los_Angeles)
            # "file","04102012_vito.zip","12324","active","ceteris_paribus","2012-10-04 12:22:01"
            if len(row) > 1:
                if row[0] == 'file' :
                    date_created = WikiSpaces.gettimestampfromwstime(row[5]) + WikiSpaces.timeoffset
                    self.filelist[row[1]] = {'name': row[1], 'size': row[2], 'username': row[5], 'date_created': date_created}

                    if (row[1] in oldfiles):
                        del(oldfiles[row[1]])

        for f in oldfiles:
            f['deleted'] = cachetime
            self.db.query('''UPDATE file SET deleted={:d} WHERE name='{}' AND spaceid={:d} AND deleted=0'''.format(cachetime, f['name'], self.spaceid))
            self.dispatch('delete', ('file', f))

        return self.filelist

    def getFileHistorylive(self, filename):
        # get history of file from webpage, as there's no SOAP API for file versions
        # TODO: make this work when pagination of history (>20 versions?) comes into play
        r = requests.get("https://{}.wikispaces.com/file/history/{}".format(self.spacename, filename))

        # Don't use below, as we do not want to get the content BLOBs here:
        # oldfiles = self.dbtable_file.distinct('id', spaceid=self.spaceid, name=filename)
        oldfiles = self.db.query('''SELECT id, name, size, type, username, spaceid, cachetime, deleted
                            FROM file
                            WHERE name='{}' AND spaceid={:d} '''.format(filename, self.spaceid))
        oldfiles = dict((f['id'], dict(f)) for f in oldfiles)
        numoldfiles = len(oldfiles)
        historyrows = Selector(text = r.text).xpath('//div[@id="WikiTableFileHistoryList"]/table/tbody/tr')

        filehistory = {}
        iserror = False
        for historyrow in historyrows:
            try:
                fileurl = "http://{}.wikispaces.com{}".format(self.spacename, historyrow.xpath('.//td[2]/a/@href').extract()[0].replace('/file/detail/', '/file/view/'))
            except IndexError:
                # WS does not deliver a 404 error in this case, just  <td colspan="5" class="noDataHolder">No page history.</td>
                iserror = True
                break
            fileuser = historyrow.xpath('.//td[5]/a[2]/text()').extract()[0].strip()
            fileversion = int(fileurl.split('/').pop())

            if (fileversion in oldfiles):
                if int(oldfiles[fileversion]['deleted']) > 0:
                    # un-deleted file:
                    oldfiles[fileversion]['deleted'] = 0
                    self.db.query('''UPDATE file SET deleted=0 WHERE id={:d} AND spaceid={:d} AND deleted>0'''
                                  .format(fileversion, self.spaceid))
                    self.dispatch('update', ('file', oldfiles[fileversion]))

                filehistory[fileversion] = oldfiles[fileversion]
                del(oldfiles[fileversion])
            else:
                # New version found
                r = requests.get(fileurl)
                if r.status_code == requests.codes.ok:
                    lastmodified = WikiSpaces.gettimestampfromwstime(r.headers.get("Last-Modified"), f = '%a, %d %b %Y %X %Z')
                    newfile = {'id': fileversion,
                               'name': filename,
                               'size': r.headers.get('Content-Length'),
                               'type': r.headers.get('Content-Type'),
                               'username': fileuser,
                               'spaceid': self.spaceid,
                               'date_created': lastmodified,
                               'deleted': 0,
                               'cachetime': now()}

                    newfile['content'] = r.content
                    self.dbtable_file.insert(newfile)

                    # We do not want to hand over content due to possible large size
                    del(newfile['content'])

                    loginfo('Downloaded {} from {}'.format(filename, fileurl))
                    if numoldfiles == 0:
                        self.dispatch('create', ('file', newfile))
                    else:
                        self.dispatch('update', ('file', newfile))

                    numoldfiles += 1
                    filehistory[fileversion] = newfile

                else:
                    logging.error('Could not download {} from {}'.format(filename, fileurl))

        if (len(oldfiles) > 0) and (not iserror):
            for f in oldfiles:
                f['deleted'] = now()
                self.db.query('''UPDATE file SET deleted={:d} WHERE id={:d} AND spaceid={:d} AND deleted=0'''
                          .format(f['deleted'], f['id'], self.spaceid))
                self.dispatch('delete', ('file', f))

        if (not filehistory == {}) and (not iserror):
            return filehistory
        else:
            logging.error('Could not getFileHistorylive({})@{:d}'.format(filename, self.spaceid))
            return None

class Subscriber:
    def __init__(self, name = None):
        self.name = self.__class__.__name__ if name is None else name
    def create(self, message):
        logging.debug('{} got create message type "{}"'.format(self.name, message[0]))
    def read(self, message):
        logging.debug('{} got read message type "{}"'.format(self.name, message[0]))
    def update(self, message):
        logging.debug('{} got update message type "{}"'.format(self.name, message[0]))
    def delete(self, message):
        logging.debug('{} got delete message type "{}"'.format(self.name, message[0]))


class GitFastEx(Subscriber):
    def __init__(self, outputdir = None):
        self.db = WikiSpaces.db  # TODO: Ceck if this is wise
        # create a - hopefully unique - string to be used as a separator/EOT mark
        self.eotsign = hashlib.sha224(urandom(64)).hexdigest()
        self.now = datetime.datetime.utcnow()
        self.outputdir = '.' if outputdir is None else outputdir
        self.pausenotifications = False

    def outfile(self, prefix = None):
        filetimestr = '{:%Y%m%d%H%M%S}'.format(self.now)
        return "{}/{}-{}.gitfastimport".format(self.outputdir, 'wiki' if prefix is None else prefix, filetimestr)

    def writegitfast(self, fastimport, prefix = None):
        if len(fastimport) > 0:
            output = open(self.outfile(prefix), 'ab')
            output.write(fastimport)
            output.close()

    def export_all_pages(self, spaceid):
        return self.export_pages(spaceid, fromdate = 0)

    def export_pages(self, spaceid, fromdate = None):
        space = self.db['space'].find_one(id=int(spaceid))
        if space is None:
            logging.error('No such spaceId {:d} in db'.format(spaceid))
            return
        else:
            space = dict(space)

        if fromdate is None:
            fromdate = int(space['date_exported_page'])
        else:
            fromdate = int(fromdate)

        query = '''SELECT p.pageId, p.versionId
                    FROM page p
                    WHERE
                        p.deleted = 0
                        AND p.spaceId={:d}
                        AND p.date_created > {:d}
                    ORDER BY p.date_created ASC'''.format(spaceid, fromdate)
        q = self.db.query(query)

        i = 0
        if not q is None:
            for page in q:
                fastimport = self.page2gitfast(page['pageId'], page['versionId'])
                self.writegitfast(fastimport, '{}-pages-{:d}'.format(space['name'], fromdate))
                i += 1

        self.db.query('UPDATE space SET date_exported_page={:d} WHERE id={:d}'.format(int(self.now.timestamp()), spaceid))
        loginfo('Exported {:d} pageversions to {}'.format(i, self.outfile('{}-pages-{:d}'.format(space['name'], fromdate))))

    def export_all_topics(self, spaceid):
        return self.export_topics(spaceid, fromdate = 0)

    def export_topics(self, spaceid, fromdate = None):
        space = self.db['space'].find_one(id=int(spaceid))
        if space is None:
            logging.error('No such spaceId {:d} in db'.format(spaceid))
            return
        else:
            space = dict(space)

        if fromdate is None:
            fromdate = int(space['date_exported_message'])
        else:
            fromdate = int(fromdate)

        query = '''SELECT m.topic_id, m.date_created
                    FROM message m
                    LEFT JOIN page p ON m.page_id = p.pageId
                    WHERE
                        m.deleted = 0
                        AND p.spaceId={:d}
                        AND m.date_created > {:d}
                    ORDER BY m.date_created ASC'''.format(spaceid, fromdate)
        q = self.db.query(query)

        i = 0
        if not q is None:
            for message in q:
                fastimport = self.topic2gitfast(message['topic_id'], message['date_created'])
                self.writegitfast(fastimport, '{}-topics-{:d}'.format(space['name'], fromdate))
                i += 1

        self.db.query('UPDATE space SET date_exported_message={:d} WHERE id={:d}'.format(int(self.now.timestamp()), spaceid))
        loginfo('Exported {:d} topicversions to {}'.format(i, self.outfile('{}-topics-{:d}'.format(space['name'], fromdate))))

    def export_all_files(self, spaceid):
        return self.export_files(spaceid, fromdate = 0)

    def export_files(self, spaceid, fromdate = None):
        space = self.db['space'].find_one(id=int(spaceid))
        if space is None:
            logging.error('No such spaceId {:d} in db'.format(spaceid))
            return
        else:
            space = dict(space)

        if fromdate is None:
            fromdate = int(space['date_exported_file'])
        else:
            fromdate = int(fromdate)

        query = '''SELECT f.id
                    FROM file f
                    WHERE
                        f.deleted = 0
                        AND f.spaceif={:d}
                        AND f.date_created > {:d}
                    ORDER BY f.date_created ASC'''.format(spaceid, fromdate)
        q = self.db.query(query)

        i = 0
        if not q is None:
            for filee in q:
                fastimport = self.file2gitfast(file['id'])
                self.writegitfast(fastimport, '{}-files-{:d}'.format(space['name'], fromdate))
                i += 1

        self.db.query('UPDATE space SET date_exported_file={:d} WHERE id={:d}'.format(int(self.now.timestamp()), spaceid))
        loginfo('Exported {:d} fileversions to {}'.format(i, self.outfile('{}-files-{:d}'.format(space['name'], fromdate))))

    def page2gitfast(self, pageid, versionid = None, linktopics = True, converter = lambda x: mdconvert(x)):
        if versionid is None:
            versionsel = ''
        else:
            versionsel = ' AND page.versionId = {:d} '.format(versionid)
        query = '''SELECT DISTINCT page.pageId, page.*, space.name AS spacename
                        FROM page
                        LEFT JOIN space ON page.spaceId = space.id
                        WHERE page.pageId = {:d} AND page.deleted = 0 {}
                        ORDER BY page.date_created DESC'''.format(pageid, versionsel)
        q = self.db.query(query)

        try:
            page = dict([l for l in q][0])
        except IndexError:
            page = None

        if not page is None:
            if page['name'] == 'space.menu':
                page['name'] = 'Sidebar'
                # fix links in space.menu
                page['content'] = page['content'].replace('[[{}/'.format(page['spacename']), '[[')
            if page['name'] == 'home':
                page['name'] = 'Home'

            fastimport = "commit refs/heads/master\n"
            fastimport += "committer {} <userid-{:d}@{}.wikispaces> {:%a, %e %b %Y %H:%M:%S} UT\n".format(page['user_created_username'], page['user_created'], page['spacename'], datetime.datetime.fromtimestamp(page['date_created']))
            fastimport += "data <<EOT{}\n".format(self.eotsign)
            fastimport += "versionId {:d}\n".format(page['versionId'])
            fastimport += page['comment'] if not page['comment'] is None else '' + "\n"
            fastimport += "EOT{}\n\n".format(self.eotsign)

            fastimport += "M 100644 inline {}.md\n".format(slugify(page['name']))
            fastimport += "data <<EOT{}\n".format(self.eotsign)
            # fastimport += "={}=\n".format(page['name'])

            fastimport += converter(page['content']) + "\n"

            topics = self.db.query('''SELECT DISTINCT message.topic_id, message.*
                        FROM message
                        WHERE message.page_id = {:d}
                            AND message.deleted = 0
                            AND subject <> ''
                            AND message.date_created <= {:d}
                        ORDER BY message.date_created ASC
                        '''.format(page['pageId'], page['date_created']))

            pagetopics = ''
            for topic in topics:
                if topic['subject'] == '':
                    topic['subject'] = topic['pagename']

                # we choose flat names, as GH wiki doesn't support dirs/namespaces/slashes in page names
                topicwikiname = "{}-Topic-{}-{:d}".format(slugify(page['name']), slugify(topic['subject']), topic['topic_id'])
                # build wiki list of topics for inclusion in wiki page
                pagetopics += "* [[{}|{}]]\n".format(topicwikiname, topic['subject'])

            if pagetopics != '':
                fastimport += '\n\n----\n\n==Topics==\n\n'
                fastimport += pagetopics

            fastimport += "EOT{}\n\n".format(self.eotsign)

            return fastimport.encode('utf-8')

    def topic2gitfast(self, topicid, dateline = None, converter = lambda x: mdconvert(x)):
        if dateline is None:
            datecut = ''
        else:
            datecut = ' AND message.date_created <= {:d} '.format(dateline)
        messages = self.db.query('''SELECT DISTINCT message.id, message.*,page.name AS pagename, space.name AS spacename
                        FROM message
                        LEFT JOIN page ON message.page_id = page.pageId
                        LEFT JOIN space ON page.spaceid = space.id
                        WHERE message.topic_id = {:d} AND message.deleted = 0 {}
                        ORDER BY message.date_created ASC'''.format(topicid, datecut))

        first = True
        for message in messages:
            if first:
                first = False
                # we choose flat names, as GH wiki doesn't support dirs/namespaces/slashes in page names
                topicwikiname = "{}-Topic-{}-{:d}".format(slugify(message['pagename']), slugify(message['subject']), message['topic_id'])
                filename = topicwikiname + ".md"

                if message['subject'] == '':
                    message['subject'] = 'Topic {}'.format(message['pagename'])

                topic_subject = message['subject']

                topictext = "={}=\n" .format(topic_subject)
                topictext += "Wiki-Page [[{}]] Discussion\n\n".format(message['pagename'])
            else:
                if message['subject'] == '':
                    message['subject'] = 'Re: {}'.format(topic_subject)

                topictext += "\n\n----\n\n"
                topictext += "==Re: {}==\n".format(message['subject'])

            topictext += "* From: {} <userid-{:d}@{}.wikispaces>\n".format(message['user_created_username'], message['user_created'], message['spacename'])
            topictext += "* Date: {:%a, %e %b %Y %H:%M:%S} UT\n".format(datetime.datetime.fromtimestamp(message['date_created']))
            topictext += "* Message-ID: <{:d}-{:d}@{}.wikispaces>\n\n".format(message['topic_id'], message['id'], message['spacename'])
            topictext += converter(message['body'])

        if not first:
            fastimport = "commit refs/heads/master\n"
            fastimport += "committer {} <userid-{:d}@{}.wikispaces> {:%a, %e %b %Y %H:%M:%S} UT\n".format(message['user_created_username'], message['user_created'], message['spacename'], datetime.datetime.fromtimestamp(message['date_created']))
            fastimport += "data <<EOT{}\n".format(self.eotsign)
            fastimport += "Import of message in topic '{}' on page '{}'\n".format(topic_subject, message['pagename'])
            fastimport += "EOT{}\n\n".format(self.eotsign)

            fastimport += "M 100644 inline {}\n".format(filename)
            fastimport += "data <<EOT{}\n".format(self.eotsign)
            fastimport += topictext
            fastimport += "\nEOT{}\n\n".format(self.eotsign)

            return fastimport.encode('utf-8')

    def file2gitfast(self, versionid):
        # TODO: Make sure file id is really uniquely provided by WikiSpaces
        q = self.db.query('''SELECT f.*, u.id AS userid, s.name AS spacename
                        FROM file f
                        LEFT JOIN user u ON f.username=u.username
                        LEFT JOIN space s ON f.spaceid=s.id
                        WHERE f.id={:d}'''.format(versionid))

        try:
            file = dict([l for l in q][0])
        except IndexError:
            file = None
            logging.error('Could not retrieve file with id {:d} from db'.format(versionid))

        if file is None:
            return
        else:
            fastimport = "commit refs/heads/master\n"
            fastimport += ''''committer {} <userid-{:d}@{}.wikispaces> {:%a, %e %b %Y %H:%M:%S} UT\n'''.format(file['username'], file['userid'], file['spacename'], datetime.datetime.fromtimestamp(file['date_created']))
            fastimport += "data <<EOT{}\n".format(self.eotsign)
            fastimport += "Import file from {}.wikispaces.com\n".format(file['spacename'])
            fastimport += "EOT{}\n\n".format(self.eotsign)

            # TODO: use slugify(filename), but don't forget to change links in Wiki accordingly?
            fastimport += "M 100644 inline {}\n".format(file['name'])
            fastimport += "data {:d}\n".format(len(file['content']))

            #fastimport += file['content']
            #fastimport += "\n\n"
            return fastimport.encode('utf-8') + file['content'] + "\n\n".encode('utf8')

    def update_message(self, message):
        if self.pausenotifications:
            return
        pass

    def create_member(self, message):
        if self.pausenotifications:
            return
        logging.debug('{} got update member event for "{}"'.format(self.name, message[0]))

def mdconvert(text):
    '''Convert WikiSpaces markup to MarkDown

    Keyword argumens:
    text -- markup text to convert
    '''
    wp = WikispacesToMarkdownConverter(text, {})
    wp.options['filelocation'] = 'files/'
    wp.options['imagelocation'] = 'files/'
    return wp.run()

def getchanges(spacename):
    '''Check http://{SPACENAME}.wikispaces.com/wiki/changes for changes.

    Keyword arguments:
    spacename -- Name of the wikispaces space name to observe
    '''

    getalltypes = set()
    if WikiSpaces.db is None:
        raise(Exception('WikiSpaces.db not opened (no Site() instance?)'))

    try:
            WikiSpaces.db.load_table('space')
    except:  # sqlalchemy.exc.NoSuchTableError as e:
        return [('space', '*'), ('page', '*'), ('message', '*'), ('user', '*'), ('file', '*')]

    spacestruct = WikiSpaces.db['space'].find_one(name = spacename)
    if spacestruct is None:
        return [('space', '*'), ('page', '*'), ('message', '*'), ('user', '*'), ('file', '*')]
    else:
        spacestruct = dict(spacestruct)

    params = {'showPage': 1,
              'showMsg': 1,
              'showComment': 0, # Not used in openv wiki
              'showFile': 1,
              'showTag': 0, # TODO: use tags?
              'showUser': 1,
              'showWiki': 0, # Not used in openv wiki
              'go': 1}
    r = requests.get("http://{}.wikispaces.com/wiki/changes".format(spacename), params)
    html = r.text

    #html=open('changes-test', 'r').read()

    links = Selector(text = html).xpath('//div[@class="col-md-9"]//strong/a[1]/@href').extract()
    dates = Selector(text = html).xpath('//div[@class="col-md-3"]//abbr[@class="WikispacesTooltip"]/@title').extract()
    latest_change = WikiSpaces.gettimestampfromwstime(dates[0], '%A, %b %d, %Y %I:%M %p')

    '''
    if spacestruct['date_changes_check'] > latest_change:
        logging.info('No changes since last check')
        return []
    '''

    nextlink = Selector(text = html).xpath('//a[@id="pagerNext"]/@href').extract()[0]
    nextlinkinfo = dict((a, b[0]) for a,b in parse_qs(urlparse(nextlink).query).items())
    logging.debug("nextlinkinfo", nextlinkinfo)
    q = WikiSpaces.db['page'].find_one(versionId = nextlinkinfo['latest_id_page'])
    if q is None:
        getalltypes.add('page')
        logging.info('Need to check all pages for new versions (page versionid {} not found)'.format(nextlinkinfo['latest_id_page']))

    q = WikiSpaces.db['message'].find_one(id = nextlinkinfo['latest_id_msg'])
    if q is None:
        getalltypes.add('message')
        logging.info('Need to check all topics for new messages (message id {} not found)'.format(nextlinkinfo['latest_id_msg']))

    q = WikiSpaces.db['file'].find_one(id = nextlinkinfo['latest_id_file'])
    if q is None:
        getalltypes.add('file')
        logging.info('Need to check all files for new versions (file id {} not found)'.format(nextlinkinfo['latest_id_file']))

    if int(nextlinkinfo['latest_id_user_add']) == 0:
        # FIXME: this looks like another bug at WikiSpaces
        
        q = WikiSpaces.db.query('''SELECT joined
                    FROM members
                    ORDER BY joined
                    LIMIT 1''')

        try:
            latestmember = dict([l for l in q][0])
        except IndexError:
            getalltypes.add('user')
            logging.info('Need to check all members for new users (no member found)')
            
        if (int(nextlinkinfo['latest_date_user_add']) + WikiSpaces.timeoffset) > int(latestmember['joined']):
            getalltypes.add('user')
            logging.info('Need to check all members for new users (latest joined in db < latest_date_user_add)')
    else:
        q = WikiSpaces.db['user'].find_one(id = nextlinkinfo['latest_id_user_add'])
        if q is None:
            getalltypes.add('user')
            logging.info('Need to check all members for new users (user id {:d} not found)'.format(nextlinkinfo['latest_id_user_add']))

    urls = list(set(links)) # make unique
    changeslist = []
    for url in links:
        # /message/view/<message.id>
        # /user/view/<user.name>
        # /file/detail/<file.name>
        # /<page.name>
        p = urlparse(url).path[1:]
        try:
            (u, _, w) = p.split('/', maxsplit = 2)
        except ValueError:
            w = p.replace('+', ' ') # TODO: Check if this replacement is OK for all pages
            u = 'page'
            pass

        if not u in ['page', 'message', 'user', 'file']:
            raise(Exception('Unknown link type "{}"'.format(u)))

        if u in getalltypes:
            # if we have to get all of this type anyway, we don't bother with single changes
            continue

        if u == 'message':
            try:
                w = w.split('/')[-1]
                message = WikiSpaces.db['message'].find_one(id = w)
            except:
                message = None
            if message is None:
                # we have message-id but we need topic-id to query via SOAP API
                location = None
                try:
                    # Get topic_id by going through redirection hell
                    r = requests.head(url, allow_redirects=True)
                    location = [resp.headers.get('Location') for resp in r.history][-1]
                except:
                    location = None

                if not location is None:
                    p = urlparse(location).path[1:]
                    (u, _, x) = p.split('/', maxsplit = 2)
                    if u != 'share':
                        raise(Exception('Unknown redirection to link type {}'.format(u)))
                    else:
                        u = 'message'

                    w = (w, x) # (messageid, topicid)
            else:
                continue

        changeslist.append((u, w))

    changeslist += [(u, '*') for u in getalltypes]
    changeslist = list(set(changeslist)) # make unique

    return changeslist


if __name__ == "__main__":
    import configparser, os

    logging.basicConfig(format = '%(asctime)s: %(levelname)s: %(message)s', level = logging.INFO)

    try:
        config = configparser.ConfigParser.SafeConfigParser({'user':'', 'password':''})
    except AttributeError:
        config = configparser.ConfigParser()

    config.read(['/etc/smtpclient.ini', os.path.expanduser('~/.smtpclient.ini')])

    if config.has_section('wikispaces'):
        section = 'wikispaces'
    else:
        try:
            section = config.sections()[0]
        except IndexError:
            section = 'DEFAULT'

    username = config.get(section, 'username')
    password = config.get(section, 'password')
    try:
        spacename = config.get(section, 'space')
    except configparser.NoOptionError:
        spacename = 'openv'

    w = Site()

    changes = getchanges(spacename)

    if len(changes) == 0:
        exit('No changes, nothing to do.')
    else:
        s = w.login(username, password)
        space = Space(spacename, s)
        space.get()

        for u, w in changes:
            if u == 'space':
                pass
            elif u == 'user':
                users = Users(s)
                if w == '*':
                    l = space.listmembers()
                    for k, v in l.items():
                        users.getUser(k)
                else:
                    space.getMember(w)
                    users.getUser(w)

            elif u == 'page':
                pages = Pages(spacename, s)
                if w == '*':
                    allpages = pages.getPageslive()
                else:
                    pages.getPageVersionslive(w)

            elif u == 'message': # topic
                if w == '*':
                    pages = Pages(spacename, s)
                    allpages = pages.listPages()

                    messages = Messages(s)
                    for page in allpages:
                        messages.getAllMessagesInPage(page['pageId'])
                        #logging.info('messages.getAllMessagesInPage({})'.format(page['name']))
                else:
                    (messageid, topicid) = w
                    message = WikiSpaces.db['message'].find_one(id = messageid)
                    if message is None:
                        messages = Messages(s)
                        messages.listMessagesInTopiclive(topicid)

            elif u == 'file':
                files = Files(spacename)
                if w == '*':
                    files.getAllFiles()
                else:
                    files.getFileHistorylive(w)

            else:
                raise(Exception('Unknown update type "{}"'.format(u)))

        WikiSpaces.db['space'].update(dict(id = space.spacestruct['id'], date_changes_check= now()), ['id'])
    '''

    space = Space(spacename, s)
    space.getlive()

    #f = Files(spacename)
    # print(f.getFileHistorylive('Platine_bestückt.jpg')) #CHECK: OK
    # print(f.getFileHistorylive('vcontrold.xml'))
    #f.getAllFiles()

    g = GitFastEx()
    #print(g.file2gitfast(257758158))
    #print(g.outfile('präfi'))
    g.export_all_pages(space.spaceid)
    #g.export_all_topics(space.spaceid)

    #print(g.outfile()) # checked OK
    #print(g.topic2gitfast(18814747, lambda x: x.replace(' ', '_')))
    print(g.page2gitfast(3087287, converter=lambda x: mdconvert(x)))
    print (mdconvert('### blah #\n sadsad ADSD \n * asdsad'))
    '''
    '''
    space.register('create', g, g.create_member)
    users = Users(s)
    l = space.listmembers()
    for k,v in l.items():
        users.getUser(k)

    #print(messages.listTopicslive(228816816))
'''

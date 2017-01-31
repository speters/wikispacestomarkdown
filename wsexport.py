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
import sys
import time

import csv
import requests
#from scrapy.selector import Selector

_lastreply = ''
def replyfilter(r):
    lastreply = r
    return r

Binding.replyfilter = (lambda s,r: replyfilter(r))

#from slugify import slugify
def slugify(s):
    return s.replace('/','_').replace('\\', '_')

loginfo = logging.info


def now():
    return int(datetime.datetime.utcnow().timestamp())

def res2dict(p):
    return [dict(m) for m in p] if not p is None else None

class WikiSpaces(object):
    urlformat = 'http://www.wikispaces.com/{}/api'
    imp = Import('http://schemas.xmlsoap.org/soap/encoding/')
    (imp.filter.add(urlformat.format(i)) for i in ('site', 'space', 'user', 'page', 'message'))
    doctor = ImportDoctor(imp)
    cachetime = 3600
    # wikispaces "stamdard" timestamp seems to be in PST (Pacific Standard Time / UTC - 8h), whileas we live in CET / UTC +1h
    # we could stay in this TZ, but then git logs also show this TZ, which is annoying
    # to adjust the timestamp to the actual displayed time of our TZ (CET), it's neccessary to substract 9h:
    timeoffset = -8*3600
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
    
    def register(self, event, who, callback=None):
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
        return spacestruct['id']

    @staticmethod
    def getspacename(spaceid):
        spacestruct = WikiSpaces.db['space'].find_one(id = spaceid)
        return int(spacestruct['name'])
    
    @staticmethod
    def gettimestamp(s, f = "%Y-%m-%d %H:%M:%S", o = None):
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
        loginfo('Logged in as user {}'.format(username))
        self.dispatch('create', ('session', self.session))
        return self.session

    def logout(self):
        self.dispatch('delete', ('session', self.session))
        self.session = None

#TODO: Split Space into Space, Member
class Space(WikiSpaces):
    url = WikiSpaces.urlformat.format('space') + '?wsdl'

    def __init__(self, spacename, session = None):
        self.spaceApi = Client(Space.url, doctor = WikiSpaces.doctor)
        self.session = session
        self.spacename = spacename
        self.init_events(['create', 'update', 'delete'])    # A bit misleading, because these are events regarding members
        self.csvmemberlist = None

        try:
            self.dbtable_space = WikiSpaces.db.load_table('space')
        except: #sqlalchemy.exc.NoSuchTableError as e:
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
                    PRIMARY KEY(id));''')
            self.dbtable_space = WikiSpaces.db.load_table('space')
            self.dbtable_space.create_index(['id'])
            logging.info('Created database table space')

        s = self.dbtable_space.find_one(name=self.spacename)
        if not s is None:
            self.spacestruct = dict(s)
            self.lastupdate = self.spacestruct['cachetime']
        else:
            self.lastupdate = 0

        try:
            self.dbtable_members = WikiSpaces.db.load_table('members')
        except: #sqlalchemy.exc.NoSuchTableError as e:
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

        self.memberlist = {}
        self.memberlist_time = 0

    def get(self):
        if (not (self.session is None)) and ((now() - self.lastupdate) > WikiSpaces.cachetime):
            return self.getlive()
        else:
            self.spacestruct = self.dbtable_space.find_one(name=self.spacename)
            if self.spacestruct is None:
                self.getlive()
            else:
                self.spacestruct = dict(self.spacestruct)
                self.lastupdate = self.spacestruct['cachetime']
            return self.spacestruct

    def getlive(self):
        space = self.spaceApi.service.getSpace(self.session.session, self.spacename)
        self.spacestruct = WikiSpaces.dict(space)
        self.lastupdate = now()
        self.spacestruct['cachetime'] = self.lastupdate
        self.spacestruct['cachetime_members'] = self.memberlist_time
        self.dbtable_space.upsert(self.spacestruct, keys=['id']) #,ensure=True)
        loginfo('Space.getlive()@'.format(self.spacename))
        return self.spacestruct

    def listmembers(self):
        if ((not (self.session is None)) and (len(self.memberlist) == 0) or ((now() - self.memberlist_time) > WikiSpaces.cachetime)):
            return self.listmemberslive()
        else:
            s = self.dbtable_members.find(spaceid=self.spacestruct['id'])
            if not s is None:
                self.memberlist = dict((m['username'], dict(m)) for m in s)
                self.memberlist_time = self.spacestruct['cachetime_members']
            else:
                self.memberlist = {}
                self.memberlist_time = 0
            return self.memberlist

    def listmemberscsvlive(self):
        r = requests.get('http://openv.wikispaces.com/wiki/members', params={'utable': 'WikiTableMemberList', 'ut_csv': 1})
        self.csvmemberlist = {}
        for row in csv.reader(r.text.split('\n'), delimiter=',', quotechar='"'):
            if len(row) > 1:
                if row[2] != 'Type':
                    joined = WikiSpaces.gettimestamp(row[1])
                    self.csvmemberlist[row[0]] = {'joined': joined, 'type': row[2]}
    
    def getmemberinfofromcsv(self, username):
        if self.csvmemberlist is None:
            self.listmemberscsvlive()
        try:
            return self.csvmemberlist[username]
        except KeyError:
            return None

    def listmemberslive(self):
        members = self.spaceApi.service.listMembers(self.session.session, self.spacestruct['id'])
        l = {}
        if len(self.memberlist) == 0:
            s = self.dbtable_members.find(spaceid=self.spacestruct['id'])
            if not s is None:
                self.memberlist = dict((m['username'], dict(m)) for m in s)
        delmembers = self.memberlist
        self.memberlist_time = now()
        for m in members:
            m = WikiSpaces.dict(m)
            m['spaceid'] = self.spacestruct['id']

            if not m['username'] in self.memberlist:
                m['joined'] = self.getmemberinfofromcsv(m['username'])['joined']
                self.dbtable_members.insert(m) #, ensure=True) # SOAP API does not deliver userId for all users, so use username
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
            d['deleted'] = self.memberlist_time
            self.dbtable_members.update(d, ['username']) #, ensure=True) # SOAP API does not deliver userId for all users, so use username
            self.dispatch('delete', ('member', m))
            loginfo('Deleted member {} @{}'.format(m['username'], m['spaceid']))

        self.memberlist = l
        self.dbtable_space.update(dict(id = self.spacestruct['id'], cachetime_members = self.memberlist_time), ['id'])
        self.spacestruct['cachetime_members'] = self.memberlist_time
        loginfo('Space.listmemberslive()@'.format(self.spacename))
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
        except: #sqlalchemy.exc.NoSuchTableError as e:
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
                    deletion INTEGER,
                    view_group VARCHAR,
                    edit_group VARCHAR,
                    comment VARCHAR,
                    content TEXT NOT NULL,
                    html TEXT,
                    date_created INTEGER,
                    user_created INTEGER,
                    user_created_username VARCHAR,
                    deleted INTEGER DEFAULT 0,
                    cachetime INTEGER,
                    PRIMARY KEY(id));''')
            self.dbtable_page= WikiSpaces.db.load_table('page')
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
            page = self.dbtable_page.find_one(spaceId=spaceid, versionId=pageversion)
        if (not(self.session is None)) and ((page is None)): # or ((now() - page['cachetime']) > WikiSpaces.cachetime)):
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
        self.dbtable_page.insert(page, keys=['pageId', 'versionId']) #,ensure=True)

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
            self.dbtable_page.update(dict(versionId = versionid, deleted = cachetime, spaceId = spaceid), ['pageId', 'versionId', 'spaceId'])
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
            self.dbtable_page.update(dict(pageId = pageid, deleted = cachetime, spaceId = spaceid), ['pageId', 'spaceId'])
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
        except: #sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE message (
                    id INTEGER,
                    subject VARCHAR NOT NULL,
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
            self.dbtable_message= WikiSpaces.db.load_table('message')
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
            self.dbtable_message.upsert(topic, keys=['id']) #,ensure=True)
        self.topiclist[pageid] = l
        
        for t in oldtopics:
            t['deleted'] = cachetime
            self.dbtable_message.update(topic_id = t['topic_id'], deleted = t['deleted'], keys=['topic_id']) #,ensure=True)
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
        except: #xml.sax._exceptions.SAXParseException:
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
                self.dbtable_message.insert(message) #,ensure=True)
                self.dispatch('create', ('message', message))
            else:
                if (message['subject'] != oldmessages[message['id']]['subject']) or (message['body'] != oldmessages[message['id']]['body']):
                    self.dispatch('update', ('message', message))
                self.dbtable_message.update(message, keys=['id']) #,ensure=True)
                del(oldmessages[message['id']])

        for t in oldmessages:
            t['deleted'] = cachetime
            self.dbtable_message.update(topic_id = t['id'], deleted = t['deleted'], keys=['id']) #,ensure=True)
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
        except: #sqlalchemy.exc.NoSuchTableError as e:
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
        if (not(self.session is None)) and ((user is None)):# or (now() - self.lastupdate) > WikiSpaces.cachetime):
            return self.getUserlive(username)
        else:
            return dict(user)

    def getUserlive(self, username):
        user = self.userApi.service.getUser(self.session.session, username)
        user = WikiSpaces.dict(user)
        user['cachetime'] = now()
        olduser = self.dbtable_user.find_one(username = user['username'])
        if olduser == None:
            self.dbtable_user.insert(user) #,ensure=True)
            self.dispatch('create', ('user', user))
        else:
            self.dbtable_user.update(user, keys=['id']) #,ensure=True)
            # self.dispatch('update', ('user', user))
        loginfo('Users.getUserlive(username="{}")'.format(username))
        return user

    def getUserById(self, userid):
        userid = int(userid)
        user = self.dbtable_user.find_one(id = userid)
        if (not(self.session is None)) and ((user is None)):# or (now() - self.lastupdate) > WikiSpaces.cachetime):
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
            self.dbtable_user.insert(user) #,ensure=True)
            self.dispatch('create', ('user', user))
        else:
            self.dbtable_user.update(user, keys=['id']) #,ensure=True)
            # self.dispatch('update', ('user', user))
        loginfo('Users.getUserByIdlive(userid="{}")'.format(userid))
        return user

def do_alltext(spacename, s):
    pages = Pages(spacename, s)
    allpages = pages.getPageslive()

    messages = Messages(s)
    for page in allpages:
        messages.getAllMessagesInPage(page['pageId'])
        logging.info('messages.getAllMessagesInPage({})'.format(page['name']))

def do_allusers(spacename, s):
    space = Space(spacename, s)
    users = Users(s)
    l = space.listmembers()
    for k,v in l.items():
        users.getUser(k)
        

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
    def __init__(self):
        pass

    def update_message(self, message):
        pass
    
    def create_member(self, message):
        logging.debug('{} got update member event for "{}"'.format(self.name, message[0]))
    
'''
echo 'http://openv.wikispaces.com/wiki/changes?latest_date_team=0&latest_date_project=0&latest_date_file=0&latest_date_page=0&latest_date_msg=0&latest_date_comment=0&latest_date_user_add=0&latest_date_user_del=0&latest_date_tag_add=0&latest_date_tag_del=0&latest_date_wiki=0&o=0' | sed -e 's/=0&/='$(date +%s)'&/g'

http://openv.wikispaces.com/space/content?utable=WikiTablePageList&ut_csv=1

http://openv.wikispaces.com/sitemap.xml
'''

if __name__ == "__main__":
    import configparser, os

    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', level=logging.INFO)

    try:
        config = configparser.ConfigParser.SafeConfigParser({'user':'', 'password':''})
    except AttributeError:
        config = configparser.ConfigParser()

    config.read(['/etc/smtpclient.ini', os.path.expanduser('~/.smtpclient.ini')])

    if config.has_section('wikispaces'):
        section = 'wikispaces'
    else:
        try:
            section=config.sections()[0]
        except IndexError:
            section = 'DEFAULT'

    username = config.get(section, 'username')
    password = config.get(section, 'password')
    try:
        spacename = config.get(section, 'space')
    except configparser.NoOptionError:
        spacename = 'openv'

    w = Site()
    s = w.login(username, password)

    space = Space(spacename, s)
    g = GitFastEx()
    space.register('create', g, g.create_member)
    users = Users(s)
    l = space.listmembers()
    for k,v in l.items():
        users.getUser(k)
   
    #print(messages.listTopicslive(228816816))
    # do_allusers(spacename, s)
    #do_alltext(spacename, s)


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
#import csv
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


# wikispaces timestamp seems to be in PST (Pacific Standard Time / UTC - 8h), whileas we live in CET / UTC +1h
# we could stay in this TZ, but then git logs also show this TZ, which is annoying
# to adjust the timestamp to the actual displayed time of our TZ (CET), it's neccessary to substract 9h:
timeoffset = -9*3600
ourtz = 'CET'

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
    db = None

    @staticmethod
    def dbconnect(dbname):
        WikiSpaces.db = dataset.connect(dbname)
        loginfo('Connected to database {}'.format(dbname))

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

    def login(self, username, password):
        self.session = Session(self.siteApi.service.login(username, password))
        loginfo('Logged in as user {}'.format(username))
        return self.session

    def logout(self):
        self.session = None

class Space(WikiSpaces):
    url = WikiSpaces.urlformat.format('space') + '?wsdl'

    def __init__(self, spacename, session = None):
        self.spaceApi = Client(Space.url, doctor = WikiSpaces.doctor)
        self.session = session
        self.spacename = spacename

        try:
            self.dbtable_space = WikiSpaces.db.load_table('space')
        except: #sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE space (
                    id INTEGER,
                    name VARCHAR,
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
                    username VARCHAR,
                    spaceid INTEGER,
                    PRIMARY KEY(id));''')
            self.dbtable_members = WikiSpaces.db.load_table('members')
            self.dbtable_members.create_index(['id'])
            logging.info('Created database table members')

        self.memberlist = []
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
        self.dbtable_space.umpsert(self.spacestruct, keys=['id']) #,ensure=True)
        loginfo('Space.getlive()@'.format(self.spacename))
        return self.spacestruct

    def listmembers(self):
        if ((not (self.session is None)) and (len(self.memberlist) == 0) or ((now() - self.memberlist_time) > WikiSpaces.cachetime)):
            return self.listmemberslive()
        else:
            s = self.dbtable_members.find(spaceid=self.spacestruct['id'])
            if not s is None:
                self.memberlist = [dict(m) for m in s]
                self.memberlist_time = self.spacestruct['cachetime_members']
            else:
                self.memberlist = []
                self.memberlist_time = 0
            return self.memberlist

    def listmemberslive(self):
        members = self.spaceApi.service.listMembers(self.session.session, self.spacestruct['id'])
        l = []
        for m in members:
            m = WikiSpaces.dict(m)
            m['spaceid'] = self.spacestruct['id']
            l.append(m)
            self.dbtable_members.upsert(m, keys=['username', 'spaceid']) #, ensure=True) # SOAP API does not deliver userId for all users, so use username
        self.memberlist = l
        self.memberlist_time = now()
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

        try:
            self.dbtable_page = WikiSpaces.db.load_table('page')
        except: #sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE page (
                    id INTEGER,
                    pageId INTEGER,
                    versionId INTEGER,
                    name VARCHAR,
                    spaceId INTEGER,
                    latest_version INTEGER,
                    versions INTEGER,
                    is_read_only BOOLEAN,
                    deletion INTEGER,
                    view_group VARCHAR,
                    edit_group VARCHAR,
                    comment VARCHAR,
                    content TEXT,
                    html TEXT,
                    date_created INTEGER,
                    user_created INTEGER,
                    user_created_username VARCHAR,
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
        self.dbtable_page.upsert(page, keys=['pageId', 'versionId']) #,ensure=True)
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

        pageversions = []
        for p in pagelist:
            page = self.getPage(p['name'], p['versionId'], p['spaceId'])
            pageversions.append(page)

        return pageversions

    def getPageslive(self, spaceid = None):
        if spaceid is None:
            spaceid = self.spaceid

        pagelist = self.listPageslive(spaceid)
        for page in pagelist:
            self.getPageVersionslive(page['name'], spaceid)
        return pagelist

    # TODO: Check for deleted or renamed pages

class Messages(WikiSpaces):
    url = WikiSpaces.urlformat.format('message') + '?wsdl'

    def __init__(self, session = None):
        self.messageApi = Client(Messages.url, doctor = WikiSpaces.doctor)
        self.session = session

        try:
            self.dbtable_message = WikiSpaces.db.load_table('message')
        except: #sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE message (
                    id INTEGER,
                    subject VARCHAR,
                    body TEXT,
                    html TEXT,
                    page_id INTEGER,
                    topic_id INTEGER,
                    responses INTEGER,
                    latest_response_id INTEGER,
                    date_response INTEGER,
                    user_created INTEGER,
                    user_created_username VARCHAR,
                    date_created INTEGER,
                    deletion INTEGER,
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
        cachetime = now()
        l = []
        for topic in topics:
            topic = WikiSpaces.dict(topic)
            topic['cachetime'] = cachetime
            l.append(topic)
            self.dbtable_message.upsert(topic, keys=['id']) #,ensure=True)
        self.topiclist[pageid] = l
        loginfo('Messages.listTopicslive(pageid="{}")'.format(pageid))
        return self.topiclist[pageid]

    def listMessagesInTopic(self, topicid):
        topic = self.dbtable_message.find(topic_id = topicid, order_by = '-cachetime')
        topic = [dict(m) for m in topic] if not topic is None else None
        latestreponse = None
        if not (topic is None):
            latestresponse = self.dbtable_message.find_one(id = topic[0]['latest_response_id'])
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
        cachetime = now()
        for message in messages:
            message = WikiSpaces.dict(message)
            message['cachetime'] = cachetime
            self.dbtable_message.upsert(message, keys=['id']) #,ensure=True)
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

        try:
            self.dbtable_user = WikiSpaces.db.load_table('user')
        except: #sqlalchemy.exc.NoSuchTableError as e:
            WikiSpaces.db.query('''
                CREATE TABLE user (
                    id INTEGER,
                    username VARCHAR,
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
        self.dbtable_user.upsert(user, keys=['id']) #,ensure=True)
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
        self.dbtable_user.upsert(user, keys=['id']) #,ensure=True)
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
    for m in l:
        users.getUser(m['username'])

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

    do_allusers(spacename, s)
    # do_alltext(spacename, s)


#!/usr/bin/python
# coding: utf-8
# vim: set fileencoding=utf-8 :

#import pdb; pdb.set_trace()
import logging
from logging import getLogger
from SOAPpy import WSDL
import SOAPpy
import datetime
from os import urandom
import sys
import hashlib
import urllib2
import csv
from scrapy.selector import Selector
from xml.sax._exceptions import SAXParseException
import getpass
#from slugify import slugify
def slugify(s):
    return s.replace('/','_').replace('\\', '_')


try:
    spacename = sys.argv[1]
    username = sys.argv[2]
except IndexError:
     print("wsexport.py spacename username [password]\n")
     print("Exports a WikiSpaces.com Wiki with files for import with git fast-import")
     exit(1)

try:
    password = sys.argv[3]
    sys.argv[3] = '***'
except IndexError:
     password = getpass.getpass(prompt="WikiSpace.com password: ")

# specify parts of export script to be run - useful when debugging
do_pages = True
do_members = True
do_messages = True
do_files = True

# save html rendered by WikiSpaces for the wiki pages?
savehtml = False

# wikispaces timestamp seems to be in PST (Pacific Standard Time / UTC - 8h), whileas we live in CET / UTC +1h
# we could stay in this TZ, but then git logs also show this TZ, which is annoying
# to adjust the timestamp to the actual displayed time of our TZ (CET), it's neccessary to substract 9h:
timeoffset = -9*3600
ourtz = 'CET'

url = 'http://www.wikispaces.com'
siteApi = WSDL.Proxy(url + '/site/api?wsdl')
spaceApi = WSDL.Proxy(url + '/space/api?wsdl')
userApi = WSDL.Proxy(url + '/user/api?wsdl')
pageApi = WSDL.Proxy(url + '/page/api?wsdl')
messageApi = WSDL.Proxy(url + '/message/api?wsdl')

# create a - hopefully unique - string to be used as a separator/EOT mark
eotsign = hashlib.sha224(urandom(64)).hexdigest()

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s' , level = logging.INFO)

memberdict = {}
pages = {}

filetimestamp = '{:%Y%m%d%H%M}'.format(datetime.datetime.now())
outputfilename = "%s.files.gitfastimport-%s" % (spacename, filetimestamp)
outputwikiname = "%s.wiki.gitfastimport-%s" % (spacename, filetimestamp)

if do_pages or do_members or do_messages:
    # delete first by overwriting empty file
    output = open(outputwikiname, 'wb')
    output.close()

    output = open(outputwikiname, 'ab')

try:
        session = siteApi.login(username, password)
except SOAPpy.Types.faultType as e:
        logging.error('Invalid Login')
        logging.error(e)

space = spaceApi.getSpace(session, spacename)
# TODO: check if this is ok or if it's better to use space.user_updated
adminuser = userApi.getUserById(session, space.user_created)
memberdict[adminuser.username] = adminuser
logging.info("Space: %s by user %s" % (space.name, adminuser.username))

if do_pages or do_messages:
    pages = pageApi.listPages(session, space.id)

    for page in pages:
        if do_messages:
            # list topics of a page
            topics = messageApi.listTopics(session, page.id)
            if topics != None:
                logging.info("Got list of topics for page %s" % page.name)
                for topic in topics:
                    # TODO: check why the following is needed. According to the docs, topic_id should contain the id of the first message
                    topic_firstmsgid = topic.id

                    topictext = ''
                    try:
                        messages = messageApi.listMessagesInTopic(session, topic.topic_id)
                    except SAXParseException:
                        logging.error("Failed on topic id %d '%s'" % (topic.topic_id, topic.subject))
                        messages = [topic]

                    if topic.subject == '':
                        topic.subject = page.name

                    # we choose flat names, as GH wiki doesn't support dirs/namespaces/slashes in page names
                    topicwikiname = "%s-Topic-%s-%d" % (slugify(page.name), slugify(topic.subject), topic.topic_id)
                    filename = topicwikiname + ".creole"

                    for message in messages:
                        if message.subject == '':
                            message.subject = topic.subject

                        if topictext != '':
                            topictext += "\n\n----\n\n"
                            topictext += "==Re: %s==\n" % message.subject
                        else:
                            topictext = "=%s=\n" % topic.subject   # TODO: Heading needed?
                            topictext += "Wiki-Page [[%s]] Discussion\n\n" % page.name

                        topictext += "* From: %s <userid-%d@%s.wikispaces>\n" %  (message.user_created_username, message.user_created, space.name)
                        topictext += "* Date: %s\n\n" % datetime.datetime.fromtimestamp(message.date_created + timeoffset).strftime('%a, %e %b %Y %H:%M:%S '+ourtz)
                        topictext += message.body

                        fastimport = "commit refs/heads/master\n"
                        fastimport += "committer %s <user-%d@%s.wikispaces> %s\n" % (message.user_created_username, message.user_created, space.name, datetime.datetime.fromtimestamp(message.date_created + timeoffset).strftime('%a, %e %b %Y %H:%M:%S '+ourtz))
                        fastimport += "data <<EOT%s\n" % eotsign
                        fastimport += "Import of message in topic '%s' on page '%s'\n" % (topic.subject, page.name)
                        fastimport += "EOT%s\n\n" % eotsign

                        fastimport += "M 100644 inline %s\n" % filename
                        fastimport += "data <<EOT%s\n" % eotsign
                        fastimport += topictext
                        fastimport += "\nEOT%s\n\n" % eotsign

                        output.write(fastimport.encode('utf-8'))
                    logging.info("Exported topic %s on page %s" % (topic.subject, page.name))

        if do_pages:
            versions = pageApi.listPageVersions(session, space.id, page.name)
            for version in sorted(versions, key = lambda revision: revision.date_created):
                pageversion = pageApi.getPageWithVersion(session, space.id, version.name, version.versionId)

                # fix links in space.menu
                if pageversion.name == 'space.menu':
                    pageversion.content = pageversion.content.replace('[[%s/' % space.name, '[[')

                fastimport = u"commit refs/heads/master\n"
                fastimport += "committer %s <user-%d@%s.wikispaces> %s\n" % (pageversion.user_created_username, pageversion.user_created, space.name, datetime.datetime.fromtimestamp(pageversion.date_created + timeoffset).strftime('%a, %e %b %Y %H:%M:%S '+ourtz))
                fastimport += "data <<EOT%s\n" % eotsign
                fastimport += "VersionId %d\n" % version.versionId
                fastimport += pageversion.comment + "\n"
                fastimport += "EOT%s\n\n" % eotsign
                try:
                    fastimport += "M 100644 inline %s.creole\n" % slugify(pageversion.name)
                except UnicodeDecodeError:
                    logging.error("UnicodeDecodeError *grmpf*")
                    print(pageversion)
                    print(fastimport)
                    fastimport += "M 100644 inline %s.creole\n" % slugify(page.name)
                fastimport += "data <<EOT%s\n" % eotsign
                # fastimport += "=%s=\n" % pageversion.name
                fastimport += pageversion.content + "\n"

                if topics != None:
                    pagetopics = ''
                    for topic in topics:
                        if topic.date_created > page.date_created:
                            # skip topic that doesn't exist yet for this page version
                            next

                        if topic.subject == '':
                            topic.subject = page.name

                        # we choose flat names, as GH wiki doesn't support dirs/namespaces/slashes in page names
                        topicwikiname = "%s-Topic-%s-%d" % (slugify(page.name), slugify(topic.subject), topic.topic_id)
                        filename = topicwikiname + ".creole"
                        # build wiki list of topics for inclusion in wiki page
                        pagetopics += "* [[%s|%s]]\n" % (topicwikiname, topic.subject)

                    if pagetopics != '':
                        fastimport += '\n\n----\n\n==Topics==\n\n'
                        fastimport += pagetopics

                fastimport += "EOT%s\n\n" % eotsign

                if savehtml:
                    # Now save also the rendered HTML...
                    fastimport += u"commit refs/heads/master\n"
                    fastimport += "committer %s <user-%d@%s.wikispaces> %s\n" % (pageversion.user_created_username, pageversion.user_created, space.name, datetime.datetime.fromtimestamp(pageversion.date_created + timeoffset).strftime('%a, %e %b %Y %H:%M:%S '+ourtz))
                    fastimport += "data <<EOT%s\n" % eotsign
                    fastimport += "VersionId %d\n" % version.versionId
                    fastimport += pageversion.comment + "\n"
                    fastimport += "EOT%s\n\n" % eotsign
                    try:
                        fastimport += "M 100644 inline %s.html\n" % slugify(pageversion.name)
                    except UnicodeDecodeError:
                        logging.error("UnicodeDecodeError *grmpf*")
                        print(pageversion)
                        print(fastimport)
                        fastimport += "M 100644 inline %s.html\n" % slugify(page.name)
                    fastimport += "data <<EOT%s\n" % eotsign
                    fastimport += pageversion.html + "\n"
                    fastimport += "EOT%s\n\n" % eotsign

                logging.info("Page %s versionId %d" % (version.name, version.versionId))
                output.write(fastimport.encode('utf-8'))

if do_files:
    outputfile = open(outputfilename, 'wb')

    # for files, and their versions, we need to scrape the info from web pages, as there is no dedicated API
    csvurl = "http://%s.wikispaces.com/space/content?utable=WikiTablePageList&ut_csv=1" % space.name
    url = urllib2.urlopen(csvurl)
    logging.info("Getting CSV info for files from %s" % csvurl)
    reader = csv.reader(url)
    i = 0
    for row in reader:
        row = [col.decode('utf8') for col in row]

        if i == 0:
            # first row
            keys = row
        else:
            values = row
            fileinfo = dict(zip(keys, values))
            if fileinfo['Type'] == 'file':
                # get history of file from webpage, as there's no SOAP API for file versions
                # TODO: make this work when pagination of history (>20 versions?) comes into play
                request = "http://%s.wikispaces.com/file/history/%s" % (space.name, urllib2.quote(fileinfo['Name'].encode('utf8')))
                try:
                    html = urllib2.urlopen(request).read()
                except urllib2.HTTPError as e:
                    logging.error('HTTPError = %s, request = %s' % (e.code, request))
                except urllib2.URLError as e:
                    logging.error('URLError = %s, request = %s' % (e.reason, request))
                except httplib.HTTPException as e:
                    logging.error('HTTPException %s' % e)

                historyrows = Selector(text=html).xpath('//div[@id="WikiTableFileHistoryList"]/table/tbody/tr')
                fileurls = [['Name', 'fileurl']]
                for historyrow in historyrows:
                    fileurl = "http://%s.wikispaces.com%s" % (space.name, historyrow.xpath('.//td[2]/a/@href').extract()[0].replace('/file/detail/','/file/view/').encode('utf8'))
                    fileuser = historyrow.xpath('.//td[5]/a[2]/text()').extract()[0].strip()
                    fileversion = fileurl.split('/').pop()

                    if not fileuser in memberdict:
                        try:
                            memberdict[fileuser] = userApi.getUser(session, fileuser.encode('utf8'))
                        except SAXParseException as e:
                            logging.error("Could not get %s" % (fileuser))

                    try:
                        response = urllib2.urlopen(fileurl)
                        data = response.read()
                        lastmodified = response.headers.get("Last-Modified")
                    except urllib2.HTTPError as e:
                        logging.error('HTTPError = %s, request = %s' % (e.code, request))
                        break
                    except urllib2.URLError as e:
                        logging.error('URLError = %s, request = %s' % (e.reason, request))
                        break
                    except httplib.HTTPException as e:
                        logging.error('HTTPException %s' % e)
                        break

                    fastimport = "commit refs/heads/master\n"
                    fastimport += "committer %s <user-%d@%s.wikispaces> %s\n" % (fileuser.encode('utf-8'), memberdict[fileuser]['id'], space.name, lastmodified)
                    fastimport += "data <<EOT%s\n" % eotsign
                    fastimport += "Import file from %s" % fileurl.encode('utf8') + "\n"
                    fastimport += "EOT%s\n\n" % eotsign

                    # TODO: use slugify(filename), but don't forget to change links in Wiki accordingly?
                    fastimport += "M 100644 inline %s\n" % fileinfo['Name'].encode('utf8')
                    fastimport += "data %s\n" % len(data)

                    fastimport += data
                    fastimport += "\n\n"

                    logging.info("File %s by %s" % (fileurl, fileuser))
                    outputfile.write(fastimport)
        i+=1
    outputfile.close()

if do_members:
    membertypes = {}
    members = spaceApi.listMembers(session, space.id)
    lastupdate = 0
    numusers = 0
    for member in members:
        if not member.username in memberdict:
            # get missing user infos (some might have been retrieved during previous operations)
            memberdict[member.username] = userApi.getUser(session, member.username)
            numusers += 1
            logging.info("Get userinfo #%d for user %s" % (numusers, member.username))

        if member.type == 'O':
            member.type = 'Organizer'
        elif member.type == 'M':
            member.type = 'Member'
        # can't change struct of SOAP object, so use another dict
        membertypes[member.username] = member.type

        if memberdict[member.username]['date_created'] > lastupdate:
            lastupdate = memberdict[member.username]['date_created']
        if memberdict[member.username]['date_updated'] > lastupdate:
            lastupdate = memberdict[member.username]['date_updated']

    # export as table in a wiki file
    fastimport = "commit refs/heads/master\n"
    fastimport += "committer %s <user-%d@%s.wikispaces> %s\n" % (adminuser.username, adminuser.id, space.name, datetime.datetime.fromtimestamp(lastupdate + timeoffset).strftime('%a, %e %b %Y %H:%M:%S '+ourtz))
    fastimport += "data <<EOT%s\n" % eotsign
    fastimport += "Import of Member-/Userlist\n"
    fastimport += "EOT%s\n\n" % eotsign

    fastimport += "M 100644 inline Memberlist.creole\n"
    fastimport += "data <<EOT%s\n" % eotsign

    # fastimport += "=%s Memberlist=\n" % space.name.encode('utf-8')
    mkeys = memberdict.values()[0]._keys()
    fastimport += "||" + "||".join(mkeys) + "||membertype||\n"

    for member in sorted(memberdict.values(), key = lambda m: m.username.lower()):
        fastimport += "||" + "||".join([str(member[val]) for val in mkeys]) + "||" + membertypes[member.username] + "||\n"

    fastimport += "EOT%s\n\n" % eotsign

    logging.info("Got memberlist")
    output.write(fastimport.encode('utf-8'))


# finally:
output.close()

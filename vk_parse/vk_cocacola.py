# -*- coding: UTF-8 -*-
import getopt, sys, os
import datetime
import dateutil.parser
import email.utils
import psycopg2
import sqlite3
import pickle
import urllib2, urlparse
import re, string, time
import HTMLParser
from BeautifulSoup import BeautifulSoup as BS, Comment
import BeautifulSoup
from pytz import timezone
import json, md5
import collections
import httplib, cookielib
import codecs


def dbopen( dbfile, exitonerr=True ):
    here = os.getcwd()
    database = dbsqli = None
    err = None

    if dbfile == None:
        if exitonerr: sys.exit( 2 )
        else: return ( False, ) * 2
    try :
        database = os.path.join( here, dbfile )
        database = sqlite3.connect( database )
        database.text_factory = str
        dbsqli = database.cursor()
    except sqlite3.Error, err :
        print str( err )
        print 'no connection with sqlite3'
    if dbsqli == None :
        print "db were not opened"
        if exitonerr: sys.exit( 2 )
        else: return ( False, ) * 4

    return database, dbsqli

def dbclose( database=False ):
    if database:
        database.commit()
        database.close()

def dbcreate(dbfile):
    conn = sqlite3.connect(dbfile)
    qry = open('cocacola.sql', 'r').read()
    c = conn.cursor()
    for st in qry.split(';'):
        st = st.strip()
        if len(st) == 0 or 'commit' in st.lower():
            continue
        c.execute(st)
    conn.commit()
    conn.close()
    return dbopen( dbfile )

def gethtml( n, topic, getvar='offset' ):
    url = "http://vk.com/" \
          + topic + "?%s=%d" % (getvar, n) 
 
    opener = urllib2.build_opener()
    opener.addheaders.append( ( 'Accept',
                                'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' ) )
    opener.addheaders.append( ( 'Accept-Language', 'ru,en-US,en;q=0.5' ) )
    opener.addheaders.append( ( 'Connection', 'keep-alive' ) )
    opener.addheaders.append( ( 'User-Agent',
                    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0' ) )

    print url
    f = opener.open( url )
    html = f.read()
    print 'html len:', len( html )
    #print html
    with open('%s.html'%topic, 'w') as f:
        f.write(html)
        f.close()
    return html


items = {}
dates = {}
today = datetime.datetime.today()
def setMoscowTZ(): os.environ['TZ'] = 'Europe/Moscow'
def setPSTTZ(): os.environ['TZ'] = 'America/Los_Angeles'
class MoscowTime():
    def __init__(self):
        setMoscowTZ()
        self.ts = datetime.datetime.today()
        self.ys = self.ts - datetime.timedelta(1)
        self.date = self.ts.strftime( "%Y/%m/%d" )
        self.time = self.ts.strftime( "%H:%M" )
        self.timedb = self.ts.strftime( "%H:%M:%S" )
        self.today = self.ts.strftime( "%d.%m" )
        self.yesterday = self.ys.strftime( "%d.%m" )
        self.y = self.ts.strftime( "%Y" )
        self.yy = self.ys.strftime( "%Y" )
        self.m = self.ts.strftime( "%m" )
        self.ym = self.ys.strftime( "%m" )
        self.d = self.ts.strftime( "%d" )
        self.yd = self.ys.strftime( "%d" )
        setPSTTZ()


class ComparableMixin(object):
    def _compare(self, other, method):
        try:
            return method(self._cmpkey(), other._cmpkey())
        except (AttributeError, TypeError):
            # _cmpkey not implemented, or return different type,
            # so I can't compare with "other".
            return NotImplemented

    def __lt__(self, other):        return self._compare(other, lambda s, o: s < o)
    def __le__(self, other):        return self._compare(other, lambda s, o: s <= o)
    def __eq__(self, other):        return self._compare(other, lambda s, o: s == o)
    def __ge__(self, other):        return self._compare(other, lambda s, o: s >= o)
    def __gt__(self, other):        return self._compare(other, lambda s, o: s > o)
    def __ne__(self, other):        return self._compare(other, lambda s, o: s != o)
    
class RuDate(ComparableMixin):
    def __init__(self, s=None, dat=None):
        self.ms = MoscowTime()
        if dat == None:
            (self.d, self.m, self.y) = self.dat_parse(s)
        else:
            (self.d, self.m, self.y) = dat.split('.')
        self.date = "%s.%s.%s" % (self.d, self.m, self.y)
        self.dbdate = "%s-%s-%s" % (self.y, self.m, self.d )
        
    def dat_parse(self, s):
        dd = s.split(' ')
        if len(dd) > 1:
            if dd[1] == u'янв': m = '01'
            elif dd[1] == u'фев': m = '02'
            elif dd[1] == u'мар': m = '03'
            elif dd[1] == u'апр': m = '04'
            elif dd[1] == u'мая': m = '05'
            elif dd[1] == u'июн': m = '06'
            elif dd[1] == u'июл': m = '07'
            elif dd[1] == u'авг': m = '08'
            elif dd[1] == u'сен': m = '09'
            elif dd[1] == u'окт': m = '10'
            elif dd[1] == u'ноя': m = '11'
            elif dd[1] == u'дек': m = '12'
            else: m = '01'
            if len( dd) > 2:
                return ("%02d" % int(dd[0]), m, dd[2])
            else:
                return ("%02d" % int(dd[0]), m, self.ms.y)
        elif s == u'вчера':
            return (self.ms.yd, self.ms.ym, self.ms.yy)
        elif s == u'сегодня':
            return (self.ms.d, self.ms.m, self.ms.y)
        else:  
            return ('01','01','1980')  
               
    def get(self):
        return self.date
    def db(self):
        return self.dbdate
    def _cmpkey(self):
        return int(self.y)*10000 + int(self.m)*100 + int(self.d)
    def __repr__(self):
        return self.date

def dbset(dbsqli, topics, topic=None):
    if topic:
        if 'wall' in topic:
            table = 'walls'
        elif 'topic' in topic:
            table = "topics"
        elif 'albom' in topic:
            print "alboms does not supported"
            sys.exit(1)
        else:
            print "wrong topic:", topic
            sys.exit(1)
        dbsqli.execute( 'select count(*) from %s where id="%s"' % (table, topic))
        (n,) = dbsqli.fetchone()
        if n:
            sql = "UPDATE %s SET " % table
            sql += " "
            sql += " where id=%s" % topic
        else:
            sql = "INSERT into %s (id, title, url, begin_date, last_date, first_message, last_message, last_time, offset) " % (table)
            sql += " VALUES (?,?,?,?,?,?,?,?,?)" 
        
    return

def add_record(dbsqli, nn, dd, tt, topic_text, topic, author, aURL, msg, msgURL, likes):
    sql = ""
    return

def items_list_wall(dbsqli, n, topic, topic_text, getvar, last=0, d=50):
    global topics, dates
    detr = BS( gethtml(n, topic, getvar) )
    [detr.script.extract() for s in detr.findAll( 'script' )]
    [detr.script.extract() for s in detr.findAll( 'javascript' )]
    '''
    pi_author = detr.findAll( 'a', {'class':'fw_reply_author'} )
    pi_text = detr.findAll( 'div', {'class':'fw_reply_text'} )
    item_date = detr.findAll( 'a', {'class':'item_date'} )
    post_item = detr.findAll( 'div', {'class':'post_item comment_item'} )
    a_all = detr.findAll( 'a')
    '''
    pi_author = detr.findAll( 'a', {'class':'pi_author'} )
    #pi_text = detr.findAll( 'div', {'class':'pi_text'} )
    pi_body = detr.findAll( 'div', {'class':'pi_body'} )
    item_date = detr.findAll( 'a', {'class':'item_date'} )
    post_item = detr.findAll( 'div', {'class':'post_item comment_item'} )
    a_all = detr.findAll( 'a')
    a_anchors = []
    i,ni,nf = 0,0,0
    nn = last+1
    dat = None
    stop = None
    count = 0
    for a in a_all:
        if 'name' in a.attrMap.keys():
            if 'reply' in a.attrMap['name']:
                a_anchors.append(a)
    
    ( dat, author, aURL, msg, msgURL ) = ('',) * 5
    for i,a in enumerate(a_anchors):
        ni = int(a_anchors[i].attrMap['name'][len('reply'):])
        if ni in items.keys():
            print ni, '(%d),'%i,
            continue 
        msgURL, dat = item_date[i].attrMap['href'],  item_date[i].text
        aURL, author = pi_author[i+1].attrMap['href'], pi_author[i+1].text
        
        dt = dat.split(u' в ')
        dd = RuDate(s=dt[0])
        tt = dt[1]
        msg = pi_body[i+1].findChild('div', {'class':'pi_text',})
        if None == msg: msg = '<picture>'
        else: msg = msg.text  
        likes = pi_body[i+1].findChild('a', {'class':'item_like _i'})
        if likes == None: likes = 0
        else: likes = likes.text
        u"Дата    Тема    URL  author    URLauth    Текст    URLcomm"
        if nf == 0 : nf = ni
        nn = ni

        #print 'dat:', dat, 'nn,last:', nn,last
        if dd >= RuDate(dat = '10.06.2013') and nn > last:  
            add_record(dbsqli, nn, dd.get(), tt, topic_text, topic, author, aURL, msg, msgURL, likes)
            items[nn] = ( topic_text, topic, nn, dd.get(), author, aURL, msg, msgURL, likes )
            topics[topic]['lastN'] = nn
            topics[topic]['count'] += 1
            count += 1
            dt = dat.split(u' в ')
            dates[nn] = {'date':dd.get(), 'time':tt}
            if topics[topic]['firstN'] == 0: topics[topic]['firstN'] = nn
        else: 
            #print dat, 'break!!!!!!!!', topic
            stop = True

    print
    if dat: print dat
    if count < d or stop: 
        stop = True
        print 'stop=True; offset:%d, count=%d, d=%d' % ( topics[topic]['offset'], count, d )
    else: 
        stop = False
        print 'stop=False; offset:%d, count=%d, d=%d' % ( topics[topic]['offset'], count,d )
        topics[topic]['offset'] += d
    return (stop, count, nf,nn,dat)

def items_list(dbsqli, n, topic, topic_text, getvar, last=0, d=20):
    global topics, dates
    detr = BS( gethtml(n, topic, getvar) )
    [detr.script.extract() for s in detr.findAll( 'script' )]
    [detr.script.extract() for s in detr.findAll( 'javascript' )]
    
    pi_author = detr.findAll( 'a', {'class':'pi_author'} )
    pi_text = detr.findAll( 'div', {'class':'pi_text'} )
    item_date = detr.findAll( 'a', {'class':'item_date'} )
    post_item = detr.findAll( 'div', {'class':'post_item'} )
    print len (post_item)
    print len (item_date)
    a_all = detr.findAll( 'a')
    
    i,ni,nf,tt = 0,0,0,''
    nn = last+1
    count = 0
    for a in a_all:
        if 'name' in a.attrMap.keys():
            if 'post' in a.attrMap['name']:
                a.attrMap['name']
                #print
                ni = int(a.attrMap['name'][len('post'):])
                if ni in items.keys():
                    print ni, 'continue...'
                    continue 
                if nf == 0 : nf = ni
                #print i, ni
                nn = ni
                item = post_item[i]
                aa = item.find('a', {'class':'pi_author'})
                #print aa['href'], aa.text
                at = item.find('div', {'class':'pi_text'})
                #print at.text
                ai = item.find('a', {'class':'item_date'})
                dt = ai.text.split(u' в ')
                dd = RuDate(s=dt[0])
                tt = dt[1]
                #print ai.attrMap['href'], ai.text
                if at == None:
                    at = item.find('div', {'class':'pi_body'})
                tt = ai.text
                if nn > last:
                    add_record(dbsqli, nn, dd.get(), tt, topic_text, topic, aa.text, aa['href'], at.text, ai.attrMap['href'], 0)
                    items[nn] = ( topic_text, topic, nn, dd.get(), aa.text, aa['href'], at.text, ai.attrMap['href'], 0 )
                    topics[topic]['lastN'] = nn
                    topics[topic]['count'] += 1
                    count += 1
                    dates[nn] = {'date':dd.get(), 'time':tt}
                    #u'вчера в 21:00'
                    #u'4 июн 2013 в 14:21'
                    if topics[topic]['firstN'] == 0: topics[topic]['firstN'] = nn

                i += 1
    
    if count > 0 or nn <= last: 
        topics[topic]['offset'] += d
        stop = False
        print 'stop=False; offset:%d, count=%d, nn=%d, last=%d, d=%d' % ( topics[topic]['offset'], count, nn, last,d )
    else: 
        stop = True
        print 'stop=True; offset:%d, count=%d, nn=%d, last=%d, d=%d' % ( topics[topic]['offset'], count, nn, last, d )
    return (stop, count, nf,nn,tt)

limit = 999999
def fulllist(dbsqli, n, topic, topic_text, getvar, last=0):
    global limit, topics
    if 'wall' in topic: flist, d = items_list_wall, 50
    else: flist, d = items_list, 20
    while True:
        stop, count, nf,nl,last_date = flist(dbsqli, n, topic, topic_text, getvar, last=last, d=d)
        if stop:
            print 'stop:', count,  topics[topic]['count'],  topics[topic]['offset'], 'items:', len(items)
            print '\n'
            break
        n = n + d
 
        print 'items:', nl - nf, "%d(%d, %d), %s, count:%d, offset:%d" % (n, nf,nl, last_date, topics[topic]['count'], topics[topic]['offset'])
        limit -= 1
        if limit == 0: break
    
            
def print_items(ofile, title=False):
    if add: fmode, utf = 'a', "utf-8"
    else: fmode, utf = 'w', "utf-8-sig" 
    f_topics = codecs.open(ofile+'_topics.csv', fmode, utf)
    f_walls = codecs.open(ofile+'_walls.csv', fmode, utf)
    print '================================'
    print 'n', 'time', 'topic', 'url', 'auth_link', 'author', 'item_link', 'message'
    
    if not add:
        f_topics.write(u'')
        f_walls.write(u'')
        if title:
            titles = (u'Тема дискуссии',u'URL дискуссии', u'#',u'Дата комментария',
                      u'Автор комментария',u'URL автора',u'Текст комментария',u'URL комментария','Likes',
                      u'Тон (негатив, позитив, нейтральный)',u'Упоминание Coca-Cola',u'Привязка к теме поста (да/нет)',
                      u'Является ли вопросом (да/нет)',u'Тема комментария',u'Комментарий ПМ/КМ')
            f_topics.write((u'%s|' * len(titles)) % titles)
            f_topics.write('\n')        
            f_walls.write((u'%s|' * len(titles)) % titles)
            f_walls.write('\n')        
    
    for k in sorted(items.keys()):
        #print
        #print k, "%s \n%s\n%s\n%s\n%s" % items[k]
        s = u'%s|http://vk.com/%s|%d|%s|%s|http://vk.com%s|%s|http://vk.com%s|%s|\n' % items[k]
        if 'topic' in items[k][1]:
            f_topics.write(s)
        elif 'wall' in items[k][1]:
            f_walls.write(s)
        else:
            print '>>> invalid record:', items[k][2]
            print s
    
    f_topics.close()
    f_walls.close()
    global topics
    f = 'cocacola.pkl'
    o = open( f, 'wb' )
    pickle.dump( topics, o )
    o.close()
    for t in topics.keys():
        if topics[t]['do']:
            print t, 'count:', topics[t]['count'], 'offset:', topics[t]['offset'], 'from:', topics[t]['firstN'], 'to:', topics[t]['lastN']
    


def itertopics(dbsqli, picklefile, newpic=True):
    global topics
    f = picklefile + '.pkl'
    if newpic or True:
        topics['topic-16297716_28039770'] = {'offset':'offset', 'name':u'КОЛЛЕКЦИЯ СТАКАНОВ COCA-COLA «СОЧИ 2014», задаём вопросы в этой теме'}
        topics['topic-16297716_28124405'] = {'offset':'offset', 'name':u'КОЛЛЕКЦИЯ СТАКАНОВ COCA-COLA «СОЧИ 2014», обмен с другими участниками группы'}
        topics['topic-16297716_22922336'] = {'offset':'offset', 'name':u'ПРИЗЫ ОТ COCA-COLA, вопросы и обсуждения'}
        topics['topic-16297716_28039770']['firstN'] = 105737
        topics['topic-16297716_28039770']['lastN'] = 111074#109672#107482#106434 #107482
        topics['topic-16297716_28039770']['offset'] = 20 * (587 - 1)
        topics['topic-16297716_28039770']['do'] = 1 
        topics['topic-16297716_28039770']['count'] = 0 
        topics['topic-16297716_28124405']['firstN'] = 105815
        topics['topic-16297716_28124405']['lastN'] = 111074#109673#107442#106393 #107442
        topics['topic-16297716_28124405']['offset'] = 20 * (18 - 1)
        topics['topic-16297716_28124405']['do'] = 1
        topics['topic-16297716_28124405']['count'] = 0
        topics['topic-16297716_22922336']['firstN'] = 105762
        topics['topic-16297716_22922336']['lastN'] = 111074#109635#107470#106435 #107470
        topics['topic-16297716_22922336']['offset'] = 20 * (347 - 1)
        topics['topic-16297716_22922336']['do'] = 1
        topics['topic-16297716_22922336']['count'] = 0
        topics['wall-16297716_192063'] = {'offset':'offset', 'name':u'Хочешь собрать коллекцию стаканов Coca-Cola «Сочи 2014»?'}
        topics['wall-16297716_201030'] = {'offset':'offset', 'name':u'Если у тебя уже есть 20 или больше баллов'}
        topics['wall-16297716_197832'] = {'offset':'offset', 'name':u'Сколько стаканов Coca-Cola «Сочи 2014» тебе уже удалось'}
        topics['wall-16297716_202374'] = {'offset':'offset', 'name':u'Некоторым счастливчикам уже удалось'}
        topics['wall-16297716_192063']['firstN'] = 0
        topics['wall-16297716_192063']['lastN'] = 0
        topics['wall-16297716_192063']['offset'] = 0
        topics['wall-16297716_192063']['do'] = 1
        topics['wall-16297716_192063']['count'] = 0
        topics['wall-16297716_201030']['firstN'] = 0
        topics['wall-16297716_201030']['lastN'] = 0
        topics['wall-16297716_201030']['offset'] = 0
        topics['wall-16297716_201030']['do'] = 1
        topics['wall-16297716_201030']['count'] = 0
        topics['wall-16297716_197832']['firstN'] = 0
        topics['wall-16297716_197832']['lastN'] = 0
        topics['wall-16297716_197832']['offset'] = 0
        topics['wall-16297716_197832']['do'] = 1
        topics['wall-16297716_197832']['count'] = 0
        topics['wall-16297716_202374']['firstN'] = 0
        topics['wall-16297716_202374']['lastN'] = 0
        topics['wall-16297716_202374']['offset'] = 0
        topics['wall-16297716_202374']['do'] = 1
        topics['wall-16297716_202374']['count'] = 0
        
        dbset(dbsqli, topics)
        o = open( f, 'wb' )
        pickle.dump( topics, o )
        o.close()
        
    else:
        picf = open( f )
        topics = pickle.load( picf )
        
        
    for t in topics.keys():
        print picklefile, ('wall' in picklefile and 'wall' in t), ('topic' in picklefile and 'topic' in t), t
        if ('wall' in picklefile and 'wall' in t) or ('topic' in picklefile and 'topic' in t):
            if topics[t]['do']:
                print t, topics[t]['offset'], topics[t]['lastN']
                topics[t]['firstN'] = 0
                topics[t]['count'] = 0
                fulllist(dbsqli, topics[t]['offset'], t, topics[t]['name'], 'offset', topics[t]['lastN'])
                print 'count:', topics[t]['count']
    print 'End itertopics' 
    

long_opts = ["db=", "n=", "l=", "topic=", "o=", "add", "limit=", "last=", "pickle="]
def main():
    try:
        opts, args = getopt.getopt( sys.argv[1:], "", long_opts )
    except getopt.GetoptError, err:
        print str( err )
        print "usage: ", long_opts
        print sys.argv
        sys.exit( 2 )

    n = 0
    last = 0
    #topic,getvar,topic_text, n = 'topic-16297716_27538909', 'post',u'',97000
    topic,getvar,topic_text, n = ('topic-16297716_28039770', 'offset',
                                  u'КОЛЛЕКЦИЯ СТАКАНОВ COCA-COLA «СОЧИ 2014», задаём вопросы в этой теме',101646)
    topic,getvar,topic_text, n = ('topic-16297716_28124405', 'offset',
                                  u'КОЛЛЕКЦИЯ СТАКАНОВ COCA-COLA «СОЧИ 2014», обмен с другими участниками группы',99926)
    topic,getvar,topic_text, n = ('topic-16297716_22922336', 'offset',
                                  u'ПРИЗЫ ОТ COCA-COLA, вопросы и обсуждения', 80)
    #topic,getvar,topic_text, n = ('wall-16297716_192063', 'offset',
    #                             u'Хочешь собрать коллекцию стаканов Coca-Cola «Сочи 2014»?',0)
    #topic,getvar,topic_text, n = ('wall-16297716_201030', 'offset',
    #                             u'Если у тебя уже есть 20 или больше баллов',0)
    
    
    #topic,getvar,topic_text, n = (u'album-16297716_173360581?act=comments', 'offset',
    #                              'Albom', 0)
    
    here = os.getcwd()
    global topics, dbsqli
    topics = {}

    if os.path.exists("cocacola.db"):
        database, dbsqli = dbopen( "cocacola.db")
    else:
        database, dbsqli = dbcreate( "cocacola.db")
    
    ofile = topic + ".csv"
    picklefile = 'output'
    global add
    add = False
    for o, a in opts:
        if o == '--pickle':
            picklefile = a 
            newpic = False
        if o == "--topic":
            topic = a
        if o == "--db":
            dbfile = a
        if o == "--n":
            n = (int(a) / 20) * 20
            print 'n=',n
        if o == "--limit":
            global limit
            limit = int(a)
            print 'n=',n
        if o == "--last":
            last = int(a)
            print 'last=',last
        if o == "--l":
            off = int(a)
            n = 20 * (off - 1)
        if o == "--o":
            ofile = a
        if o == "--add":
            add = True

    
    itertopics(dbsqli, picklefile, newpic=newpic)
    #fulllist(n, topic, topic_text, getvar, last)
    dbclose(database)
    print_items('ouput')

if __name__ == '__main__':
    main()

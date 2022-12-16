#! python2
# -*- coding: utf-8 -*-
# author: Andrzej Kryński   
# licence: MIT
'''
This is webcrawler. It fetches a persons data from
http://katalog.bip.ipn.gov.pl a Polish resource
of information about agents, members of the authorities, 
persons repressed in years 1945-1989
'''
print __doc__
def enum(**enums):
    return type('Enum', (), enums)
katalog = enum(kierownicze=4,funkcjonariusze=5,publiczne=3,rozpracowywane=2)

sesja = None
catalog = None
pages = []
link2names = []
slownik = {u"Imiona:":"imiona",u"Nazwisko:":"nazwisko",u"Nazwisko rodowe:":"nazwisko_rodowe",u"Miejsce urodzenia:":"miejsce_urodzenia",
u"Data urodzenia:":"data_urodzenia",u"Imię ojca:":"imie_ojca",u"Imię matki:":"imie_matki",
u"Dodatkowe informacje:":"dodatkowe_informacje",u"Znany też jako:":"znany_jako",u"Znany\a też jako:":u"znany_jako",u"Znany/a też jako:":u"znany_jako",
u"Kraj urodzenia:":"kraj_urodzenia"}


def main(argv):   
    '''
    The Institute of National Remembrance divided its resources into four thematic catalogs.
    In the html code, the pages have their own names, indexes and descriptions:
    "/ kierownicze-stanowiska/" catalog = 4 catalog of party and state leadership positions
    "/ funkcjonariusze/" catalog = 5 directory of security officers
    "/ osoby-publiczne/" catalog = 3 people holding public functions
    "/ osoby-rozpracowywane/" catalog = 2 directory of people "developed"
    '''

    #=============================================================================
    logging.basicConfig(filename="ipnXthreaded.log",level=logging.DEBUG,
                         format='[%(levelname)s] (%(threadName)-10s) %(message)s',
						 filemode='w'   #overwrites an old log file
                        )
    
    #=============================================================================
    try:
        opts, args = getopt.getopt(argv,"hsk:",["katalog="])
    except getopt.GetoptError:
        print ('script_name.py -k <typ> [-s]')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("Usage: script_name.py -k <typ>\nAs typ enter one of: kierownicze,"),
            print ("funkcjonariusze, publiczne, rozpracowywane.\n"),
            print ("To print statistics use option -s:  script_name.py -s\n"),
            print ("for more info read the readme.txt")
            sys.exit()
        elif opt in ("-k", "--kkatalog"):
            global catalog
            global tablename
            tablename = arg
            if arg=='kierownicze':
                catalog=katalog.kierownicze
            elif arg=='funkcjonariusze':
                catalog=katalog.funkcjonariusze
            elif arg=='publiczne':
                catalog=katalog.publiczne
            elif arg=='rozpracowywane':
                catalog=katalog.rozpracowywane
        elif opt == '-s':
            make_stats()
            exit(0)
    if (catalog != None):
        print ('Spidering catalog number %d'%(catalog))
        start()
        #findPerson()
    else:
        print (' No valid arguments found. Type -h for help')
        sys.exit(2)
        
def getSoup(query, timeout=None):
    '''simply outputs info queried from BeautifulSoup'''
    global sesja
    try:
        res = sesja.get(query)
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        print (unicode(e))
        print ("Error accessing page " + str(query))
        return   
    soup = BeautifulSoup(res.text)
    return BeautifulSoup(str(soup))    

def howManyPages(html_links_table):
    '''counts how many pages the catalog includes
    returns number of pages
    links to pages are stored in global pages table'''
    prog = re.compile("^\?simply-catalog=%d\S+"%(catalog))
    global pages
    pages = [] #cleaning the list
    for item in html_links_table:
        link = item.get('href',None)
        if prog.match(link):
            if link not in pages:
                pages.append(link)
    return len(pages)
    
def getInfo(session, page_link):
        '''fetches data from personal records'''
        
        global slownik  #why not local??????????????????????
        query = 'http://katalog.bip.ipn.gov.pl'+page_link
        info_soup = getSoup(query)
        if info_soup:
           info = info_soup.find("article",{"class" : "type-page hentry clearfix"})           # <<<<<<< none type has no attr find !!!!!
           bolds = info.findAll("strong")
           records = {slownik[tag.text]:tag.nextSibling for tag in bolds if tag.text in slownik.keys()}
           try:
              session.add(Person(**records))
              session.commit()
           except:
              session.rollback()

def getPersonalityLinks(pageNr):
        '''gets links to personal records'''
        query = 'http://katalog.bip.ipn.gov.pl/szukaj_zaawansowane/?simply-catalog=%d&page=%d'%(catalog, pageNr)
        global link2names
        soup = getSoup(query)
        linkElems = soup('a')        
        prog = re.compile("^/\informacje/\d+")
        for item in linkElems:
            link = item.get('href',None)
            if prog.match(link):
                if link not in link2names:
                    print "Adding record ",(str(link))
                    link2names.append(link)
        return
        
def start():
        ''' Fetches a pure html version of the main page for specyfied catalog.
        We are interested in links to pages only, because the list of personality links 
        is here not complet.'''
        global sesja
        sesja = requests.Session()
        
        class PersonalityLinksThread(threading.Thread):
            def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
                threading.Thread.__init__(self, group=group, target=target, name=name)
                self.args = args
                self.kwargs = kwargs
                self.queue = self.kwargs['queue']
            def run(self):
                logging.debug('running thread with %s and %s', self.args, self.kwargs)
                #getPersonalityLinks(self.kwargs['page'])
                while True:
                    try:
                        getPersonalityLinks(self.queue.get(block=True))
                    finally:
                        self.queue.task_done()
                        print "PersonalityLinksThread done"
                        
        class GetInfoThread(threading.Thread):
            def __init__(self, queue):
                threading.Thread.__init__(self)
                self.queue = queue
            def run(self):
                while True:
                    connection, link = self.queue.get(block=True)
                    logging.debug('thread gets data from %s', link)
                    try:
                        getInfo(connection, link)
                    finally:
                        self.queue.task_done()
                        print "GetInfoThread done"
                

        engine = create_engine('sqlite:///ipn.db', connect_args={'check_same_thread':False}, poolclass=QueuePool, encoding='utf-8')
        #'sqlite:///:memory:', echo=True)

        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        
        query = 'http://katalog.bip.ipn.gov.pl/szukaj_zaawansowane/?simply-catalog=%d&page='%(catalog)
        soup = getSoup(query)
        linkElems = soup('a')
        # Create a queue to communicate with the worker threads
        no_pages = howManyPages(linkElems)
        queue1 = Queue()
        # Create 8 worker threads
        for x in range(8):
            worker = PersonalityLinksThread(name="PersonalityLinksThread",kwargs={'queue':queue1})
            # Setting daemon to True will let the main thread exit even though the workers are blocking
            worker.daemon = True
            worker.start()
        for page in range(no_pages): #no_pages is the variable we have to change to a low value when testing  <<<<<<<<<<
            queue1.put(page+1,block=True)
        # Causes the main thread to wait for the queue to finish processing all the tasks
        queue1.join()
        # Here is the best - we are getting pretty personal informations to our own database
        queue2 = Queue()
        for x in range(8):
            worker = GetInfoThread(queue2)
            worker.daemon = True
            worker.start()
        for link in link2names:
            queue2.put((session, link),block=True)
        queue2.join()
        session.close()
        
if __name__ == '__main__':
        #import timeit
        import re, requests, sys, codecs, time, getopt, logging #bs4, webbrowser, io
        from BeautifulSoup import BeautifulSoup
        from Queue import Queue
        #from threading import Thread, Lock
        import threading
        import sqlite3 #have to be the latest version because of sqlalchemy
        from sqlalchemy.pool import StaticPool, QueuePool
        from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
        from sqlalchemy.sql import text
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.ext.declarative import declarative_base
        
        Base = declarative_base()
        
        class Person(Base):
            
            __tablename__ = 'kierownicze'            #minimalny wymóg klasy
      
            id = Column(Integer, primary_key=True)     #minimalny wymóg klasy
            imiona = Column(String)
            nazwisko = Column(String)
            nazwisko_rodowe = Column(String,nullable=True)
            miejsce_urodzenia = Column(String)
            kraj_urodzenia = Column(String,nullable=True)
            data_urodzenia = Column(String) #(storage_format="%(day)02d-%(month)02d-%(year)04d",
                                 #'''regexp=re.compile("(?P<day>\d+)-(?P<month>\d+)-(?P<year>\d+)")''')
                            #)
            imie_ojca = Column(String)
            imie_matki = Column(String)
            znany_jako = Column(String,nullable=True)
            dodatkowe_informacje = Column(String(400),nullable=True)
    
            def __repr__(self):
                return "<Person(name='%s', fullname='%s', born='%s', city='%s')>" % ( 
                        self.imiona, self.nazwisko, self.data_urodzenia, self.miejsce_urodzenia)
        
        main(sys.argv[1:])
        
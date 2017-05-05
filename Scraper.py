import csv
import requests
import sys
import multiprocessing
import datetime
import re

from BeautifulSoup import BeautifulSoup

def parser(page):
     '''
    return bool type for the presence of jquery.js
    '''
    soup = BeautifulSoup(page)
    for i in soup.findAll("script"):
        _src = i.get("src", "")
        if 'jquery.js' in _src:
            return True
    return False
    
def worker_logic(doc_q, stdout_lock, afd, rfd):
    while True:
        url = doc_q.get()
        html = requests.get(url).content
    	is_valid_page =  parser(html)
        with stdout_lock:
            if is_valid_page:
                print 'Accepted:', url
                afd.writelines(url+"\n")
                afd.flush()
            else:
                print 'Rejected:', url
                rfd.writelines(url+"\n")
                rfd.flush()
        doc_q.task_done()


class JqueryScraper:
	def __init__(self):   
		self.count = 0
		self.start = datetime.datetime.now()
        	self.doc_q = multiprocessing.JoinableQueue(100)
        	self.stdout_lock = multiprocessing.Lock()
        	self.input_fd = open('urls.csv')
        	self.accept_fd = open('accepted.csv', 'wb')
        	self.reject_fd = open('rejected.csv', 'wb')
        	self.workers = []
        	for _ in range(multiprocessing.cpu_count()):
            		p = multiprocessing.Process(target=worker_logic, 
                                        args=(self.doc_q, self.stdout_lock, 
                                              self.accept_fd, self.reject_fd))
            	p.start()
            	self.workers.append(p)

        	try:
            		self.run()
        	finally:
            		sys.stderr.write("Terminating workers...")
            		for i, worker in enumerate(self.workers):
                		try:
                    			worker.terminate()
                		except Exception:
                    			sys.stderr.write("Unable to terminate worker %s !" % i)

	def run(self):
		cin = csv.reader(self.input_fd)

        	for line in cin:
            		line = line[0]
            		sys.stderr.write('Retrieving %s.\n' % line)
            		self.distribute(line)
            		sys.stderr.write('%s documents parsed. %s doc/s.\n' % (self.count, self.count // (datetime.datetime.now() - self.start).total_seconds()))

        		# Wait for all the workers
        		self.doc_q.join()

	def distribute(self, line):
		self.doc_q.put(line)
        	self.count += 1



if __name__ == '__main__':
	JqueryScraper()


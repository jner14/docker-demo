import sys
print('The python version is', sys.version)
import os
print('The current working directory is', os.getcwd())
import urllib.request
import urllib.error
import logging
import lxml
import re
import pandas as pd
import sys
from time import sleep, time
from datetime import datetime as dt
from multiprocessing import Process, Queue, cpu_count
from bs4 import BeautifulSoup as bs


def main(workers=0, limit=None):
    globs = Globs()

    # Configure logging
    logging.basicConfig(
        level=logging.ERROR,
        filename='main.log',
        format='%(relativeCreated)6d %(threadName)s %(message)s')
    logging.error('STARTING NEW ENTRY AT %s' % str(dt.now()))

    # Load settings
    with open('settings.txt', 'r') as f:
        try:
            filename = f.readline().split('=')[1].strip()
            print('Loading', filename)
        except Exception as e:
            print('Failed to read filename from settings.txt'.upper(), e)
            sys.exit()
        try:
            globs.set('minmatch', int(f.readline().split('=')[1].strip()))
            print('Minmatch set to', globs.get('minmatch'))
        except Exception as e:
            print('Failed to read minimum from settings.txt'.upper(), e)
            sys.exit()

    # Load csv
    try:
        df = pd.read_csv(filename, header=0)
    except Exception as e:
        print('Failed to read csv file with filename', filename, 'from settings.txt', e)
        sys.exit()

    # Limit as needed
    if limit:
        df = df.iloc[:limit]

    # Determine number of workers(threads) to use
    if workers == 0:
        workers = cpu_count()

    # Create jobs
    st = time()
    jobs = []
    q = Queue()
    for n in range(workers):
        batch_size = len(df) // workers + 1
        start = n * batch_size
        end = start + batch_size
        p = Process(target=start_worker, args=(n, df.iloc[start: end], globs, q))
        jobs.append((n, p))
        print('Starting worker', n)
        p.start()

    # Wait until all links are scraped
    while q.qsize() < len(df):
        print('Main process is waiting for workers to finish...')
        sleep(10)

    for n, p in jobs:
        p.join(timeout=.1)
        print('Worker %s process has joined' % n)

    # Get worker results and save as a csv called results.csv
    results = pd.DataFrame(columns=['Email', 'Results'])
    i = 0
    while q.qsize() > 0:
        r = q.get(timeout=.2)
        # print(i, r)
        results.loc[len(results)] = r
        i += 1

    results.to_csv('results.csv')
    print('Results written to results.csv...')

    print("Time Elapsed: {:.2f}s".format(time() - st))


def start_worker(worker_id, links, globs, q):
    worker = Worker(worker_id, globs, q)
    worker.get_links(links)
    print('Worker %s has finished' % worker_id)
    return True


class Worker(object):

    def __init__(self, wid, globs, q):
        self.minmatch = globs.get('minmatch')
        self.link_cnt = 0
        self.id = wid
        self.result = None
        self.q = q
        super().__init__()

    def get_links(self, links):
        for k, v in links.iterrows():
            print('Worker %s is accessing %s' % (self.id, v.URL))
            try:
                raw = urllib.request.urlopen(v.URL)  #.read().decode('utf-8')
            except urllib.error.HTTPError as e:
                print('Worker %s ran into error 418 at %s' % (self.id, v.URL))
                logging.error(v.URL + str(e))
                self.q.put((v.Email, '418 error'))
                continue
            except UnicodeDecodeError as e:
                print('Worker %s ran into decoding error at %s' % (self.id, v.URL))
                logging.error(v.URL + str(e))
                self.q.put((v.Email, 'decoding error'))
                continue
            except urllib.error.URLError as e:
                print('Worker %s ran into url error at %s' % (self.id, v.URL))
                logging.error(v.URL + str(e))
                self.q.put((v.Email, 'url error'))
                continue
            except Exception as e:
                print('Worker %s ran into unknown error at %s' % (self.id, v.URL))
                logging.error(v.URL + str(e))
                self.q.put((v.Email, 'unknown error'))
                continue
            soup = bs(raw, 'lxml')
            data = soup.findAll(text=True)
            username = v.Email.split('@')[0]
            # p = re.compile("(.{15}%s.{15})" % username[:self.minmatch], re.IGNORECASE)
            # m = p.match(raw)
            result = [x.strip() for x in filter(visible, data)]
            result = [x for x in result if (username[:self.minmatch] in x.lower() or username[-self.minmatch:] in x.lower()) and '@' not in x]
            result = '; '.join(list(set(result)))
            self.q.put((v.Email, result))
            # print('pass')


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True


class Globs(object):
    """docstring for Globs"""

    def __init__(self):
        super(Globs, self).__init__()
        self.args = {}

    def get(self, arg_name):
        if arg_name in self.args.keys():
            return self.args[arg_name]
        else:
            return None

    def set(self, arg_name, value):
        self.args[arg_name] = value


if __name__ == '__main__':
    main(workers=32, limit=None)

    print('Finished!')

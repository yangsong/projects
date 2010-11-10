#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#Author: alvayang <alvayang@tabex.org>
#Last Change:
#Description:

import sys
import os
import traceback
import urllib2
import urllib
import libxml2
try:
    import chardet
except:
    pass

urls = '/geturl'


def get_resource(title, url):
    try:
        opener = urllib2.build_opener()
        opener.addHeaders = [
                ('Host',   'mp3.sogou.com'),
                ('User-Agent', 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3'),
                ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
                ('Accept-Language', 'zh-cn,en-us;q=0.7,en;q=0.3'),
                ('Accept-Encoding', 'gzip,deflate'),
                ('Accept-Charset', 'gb18030,utf-8;q=0.7,*;q=0.7'),
                ('Keep-Alive', '115'),
                ('Connection', 'keep-alive'),
                ('Referer', 'http://mp3.sogou.com/')]
        ureq = urllib2.Request(url)
        urllib2.install_opener(opener)
        f = opener.open(ureq)
        buf = ''
        z = f.read(4096)
        while z:
            buf += z
            z =  f.read(4096)
        # 解析XML
        data = buf.decode('gb2312')
        parse_xml(title, buf)
    except:
        #print traceback.format_exc()
        sys.exit()

def parse_xml(title, text):
    encoding = 'gbk'
    options = libxml2.HTML_PARSE_RECOVER + libxml2.HTML_PARSE_NOWARNING + libxml2.HTML_PARSE_NOERROR
    doc = libxml2.readDoc(text, None, encoding, options).doc
    ctxt = doc.xpathNewContext()
    items = ctxt.xpathEval(u'//downloadList')
    out = []
    ret = {}
    want = [u'size', u'urls', u'urlsource']
    if items:
        for z in items:
            for q in want:
                if q == "urls":
                    durl = (z.xpathEval(q)[0].get_content()).decode('utf-8') if z.xpathEval(q) else u'没有合理的解释'
                    newfilename = (urllib.unquote_plus(title.decode('utf-8').strip())  + "." + (durl.split(".")[-1]))
                    print "Downloading:" + newfilename
                    #newfilename = newfilename.decode('utf-8', 'ignore')
                    #newfilename = newfilename.encode('utf-8', 'replace')
                    cmd = "axel " + " \"" + durl + "\" --output=\"/opt/music/" + newfilename.replace(" ", "") + "\" && mplayer /opt/music/" + newfilename.replace(" ", "")
                    #cmd = "cd /opt/music && curl " + " \"" + durl + "\" -o \"" + newfilename + "\" && cd -"
                    try:
                        print cmd
                        os.system(cmd.encode('utf-8'))
                        return
                    except:
                        print traceback.format_exc()

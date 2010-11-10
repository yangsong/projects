#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#Author: alvayang <alvayang@tabex.org>
#Last Change:
#Description:

import sys
#from template import indextmpl
#from template import badresourcetmpl
#from twisted.internet.threads import deferToThread
#from twisted.web.client import getPage
import traceback
import urllib2
import urllib
import libxml2

import geturl

urls = '/search'
request = u"http://mp3.sogou.com/coo/oem/oem_music.so?query=%s&page=%s"

def get_resource(key, page=1):
    try:
        #gb_key = key.decode('utf-8').encode('gb2312')
        # shit, 似乎少了header不能成事阿,shit it
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

        gb_key = urllib.quote(urllib.unquote_plus(key).decode('utf-8').encode('gb2312'))
        url = request % (gb_key, str(page))
        ureq = urllib2.Request(url)
        urllib2.install_opener(opener)
        f = opener.open(ureq)
        buf = ''
        z = f.read(4096)
        while z:
            buf += z
            z =  f.read(4096)
        # 解析XML
        oparse_xml(buf, key, page)
    except:
        print traceback.format_exc()
        #stand_error(q)
        sys.exit()

def stand_error():
    ret["status"] = False
    ret["msg"] = "系统错误!"

def oparse_xml(text, key, page):
    encoding = 'gbk'
    options = libxml2.HTML_PARSE_RECOVER + libxml2.HTML_PARSE_NOWARNING + libxml2.HTML_PARSE_NOERROR
    doc = libxml2.readDoc(text, None, encoding, options).doc
    ctxt = doc.xpathNewContext()
    items = ctxt.xpathEval(u'//rss/sogouresult/item')
    out = []
    ret = {}
    want = [u'title', u'album', u'size', u'type', u'artist', u'urls']
    if items:
        print "请选择要下载的项目："
        for z in items:
            o = {}
            for q in want:
                #                if q == "urls":
                #                    url = (z.xpathEval(q)[0].get_content()).decode('utf-8') if z.xpathEval(q) else u'没有合理的解释'
                #                    print (z.xpathEval(q)[0].get_content()).decode('utf-8') if z.xpathEval(q) else u'没有合理的解释'
                #                    geturl.get_resource(url)
                o[q] = (z.xpathEval(q)[0].get_content()).decode('utf-8') if z.xpathEval(q) else u'没有合理的解释'
            out.append(o)
        index = 0
        for z in out:
            print u"[" + str(index) +u"][" + z["title"]+ u"][" + z["type"]+ u"]["+ z["artist"]+ u"][" + z["album"] + u"]:" + z["size"]
            index += 1
        zz = raw_input()
        if zz.strip().lower() == "n":
            # that means next page
            return get_resource(key, page + 1)
        if zz.strip().lower() == "p":
            return get_resource(key, page - 1)
        z = out[int(zz)]
        print z
        geturl.get_resource(z["title"].encode('utf-8'), z["urls"])
        ret["status"] = True
        ret["msg"] = out
    else:
        ret["status"] = False
        ret["msg"] = u'没有搜索到结果'


if __name__ == "__main__":
    song_name = sys.argv[1]
    get_resource(song_name)


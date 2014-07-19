#-*-coding:utf-8-*-
#!/usr/bin/python
import urllib2
import gzip
import StringIO

#urllib2抓取网页时，有时候响应体是gzip压缩的，所以需要处理一下
def fetch_url(url):
    try:
        request = urllib2.Request(url)
        request.add_header('Accept-encoding', 'gzip')
        opener = urllib2.build_opener()
        r = opener.open(request)
        isGzip = r.headers.get('Content-Encoding')
#        print 'Content-Encoding:' + isGzip
        if isGzip:
            compressed_data = r.read()
            compressed_stream = StringIO.StringIO(compressed_data)
            gzipper = gzip.GzipFile(fileobj = compressed_stream)
            data = gzipper.read()
        else:
            data = r.read()
        return data
    except Exception,e:
        print e

if __name__ == '__main__':
    print fetch_url('http://www.baidu.com')

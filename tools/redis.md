
**Redis用法简介**
---

ubuntu下安装：`sudo apt-get install redis-server`

python redis安装：`sudo apt-get install python-redis`

----

+ *python api用法*

---

```python
#!/usr/bin/python

import sys
import os
import json
import redis

DIR = sys.path[0]
redis = redis.Redis('127.0.0.1',6379)
print redis.get('test_key')

fd = open(DIR + '/books.json', 'r')
books = [json.loads(line.strip()) for line in fd.readlines()]
fd.close()

for book in books:
    book_id = books['id']
    redis.set(book_id, 1)
```

+ *命令行*

---

[redis-cli命令](http://blog.csdn.net/yhl27/article/details/9936189)

    注：keys命令可以查看key的情况，可以使用*做通配符，如keys a*

+ *配置信息*

---

[redis配置详解](http://www.cnblogs.com/wenanry/archive/2012/02/26/2368398.html)

    注：注意redis的持久化方法


> redis常用来做缓存，但还有很多[其他的应用场景](http://os.51cto.com/art/201107/278292.htm)

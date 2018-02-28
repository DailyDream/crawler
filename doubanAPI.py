# coding:utf-8
#author__:Finch
import requests
import time
import re
import pymssql
import sys

class Mssql:
    def __init__(self, config):
        self.cf = config

    def __Connect(self):
        try:
            self.conn = pymssql.connect(host=self.cf['host'], user=self.cf['user'], password=self.cf['pwd'],
                                        database=self.cf['db'])
            cur = self.conn.cursor()
        except Exception, err:
            print "Error decoding config file(connext): %s" % str(err)
            sys.exit(1)
        return cur

    def select(self, sql):
        try:
            cur = self.__Connect()
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
            self.conn.close()
            return rows
        except Exception, err:
            print "Error decoding config file(select): %s" % str(err)
            sys.exit(1)

    def insert(self, sql):
        try:
            cur = self.__Connect()
            cur.execute(sql)
            cur.close()
            self.conn.commit()
            self.conn.close()
        except Exception, err:
            print "Error decoding config file(insert): %s" % str(err)

#用于比较两个文档内容是否一致，参数为all_bookids，old_bookid这两个文档路径
def is_not_equal(all,old):
    old_id = set()
    all_id = set()
    with open(all) as all_reader:
        for all_line in all_reader:
            all_id.add(all_line)
    with open(old) as old_reader:
        for old_line in old_reader:
            old_id.add(old_line)
    if len(all_id-old_id ) == 0:
        print old_id
        print all_id
        return True
    else:
        return False

#用于判断文档是否为空
def is_not_empty(txt):
    with open(txt) as reader:
        if reader.read() =="":
            return True
        else:
            return False

#用于将文档中的id存入集合中
def from_txt_to_set(txt):
    ids=set()
    if is_not_empty(txt):
        print "该文档为空，无法存入集合中"
    else:
        with open(txt) as all_reader:
            for line in all_reader:
                ids.add(line)
        return ids

# txt:文档路径；patter：写入模式（更新或追加）; id：图书编号
def from_id_to_txt(txt,pattern,id):
    with open(txt,pattern) as writer:
        writer.writelines(id)

def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }
    # 链接SQL Server
    config = {'host': '210.38.220.120', 'user': 'sa', 'pwd': '12345678', 'db': 'doubanread'}
    mssql = Mssql(config)
    #all_bookids文档存放所有的图书编号；new_bookid存放还未爬取的图书编号；old_bookid存放已经爬去的图书id
    new_bookid=r'.\new_bookid.txt'
    old_bookid=r'.\old_bookid.txt'
    all_bookid=r'.\all_bookids.txt'
    if is_not_equal(all_bookid,old_bookid):
        print "数据已爬取完毕，中断程序"
        sys.exit(1)
    else:
        #区分是否为第一次爬取，第一次：从all_bookids这个文本中导入id；非第一次：从new_bookid这个文本导入
        if is_not_empty(new_bookid):
            #第一次使用，把all_bookids这个文本文档中的图书编号都读进ids这个集合中
            ids=from_txt_to_set(all_bookid)
            print "第一次使用"
        else:
        #old_bookid里面没有id说明是第一次爬取，有id说明不是第一次,从new_bookid这个文档中读取图书编号到ids()中
            ids=from_txt_to_set(new_bookid)
            print "非第一次使用"
        n=len(ids)
        counter = 0
        for i in range(0, n):
            time.sleep(3)
            try:
                # 计数器到达100之后，把还未爬取的ids剩下的图书编号，放进new_bookid这个文本中
                if counter == 100:
                    from_id_to_txt(new_bookid, 'w', ids)
                    sys.exit(1)
                bookid = ids.pop()
                # 把bookid追加写进old_bookid文本文档中,用于存储爬取过的ids
                from_id_to_txt(old_bookid, 'a', bookid )
                #使用豆瓣的API对数据进行获取
                url = "https://api.douban.com/v2/book/" + str(bookid)
                r = requests.get(url, headers=headers)
                counter += 1
                dict = eval(r.content)
                bookurl = "https://book.douban.com/subject/" + bookid
                title = dict['title']
                pubdate = dict['pubdate']
                author = dict['author'][0]
                author_intro = dict['author_intro']
                publisher = dict['publisher']
                summary = dict['summary']
                catalog = dict['catalog']
                if catalog =="" or  bool(re.match(r'[A-Za-z]',catalog)):
                    continue
                # insert sql 传参数插入 ps: %s作为传str类型的参数时，需要加引号
                sql = "insert into dbo.test1 values ('%s','%s','%s'" \
                      ",'%s','%s','%s','%s','%s','%s')"
                parms = (bookid,bookurl,title,pubdate,author,
                         author_intro,publisher,summary,catalog)
                mssql.insert(sql % parms)
                print( "success insert: "+str(counter)+"条记录")
                if len(ids) ==0:
                   print "数据爬取完毕"
                   sys.exit(1)
            except Exception,e:
                print "crawl failed: "+bookid
                print e
                #爬虫中断，将未爬取的url放入new_bookid这个文本中，使用覆盖的模式
                from_id_to_txt(new_bookid,'w',ids)
                sys.exit(1)
    # select sql
    # sql = "select * from Books"
    # rows = mssql.select(sql)
    # for i in range(0,len(rows)):
    #    for j in range(0,len(rows[i])):
    #        print(rows[i][j])
    #    print("*" * 200)


if __name__ == "__main__":
    main()


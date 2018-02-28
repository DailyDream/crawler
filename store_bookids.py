# coding:utf-8
# author__:Finch
from bs4 import BeautifulSoup
from urllib2 import urlopen
from doubanAPI import is_not_empty
from doubanAPI import from_id_to_txt
import time
import re

def get_mulu_Links():
    mulu_Links=set()
    mulu_Links.add("https://book.douban.com/tag/%E7%BC%96%E7%A8%8B?start=" + str(999) + "&type=T")
    for i in range(0, 1000, 20):
        links="https://book.douban.com/tag/%E7%BC%96%E7%A8%8B?start=" + str(i) + "&type=T"
        mulu_Links.add(links)
    return mulu_Links

def get_book_id():
    bookid=set()        #使用集合是因为集合中的元素具有不可重复的特点
    book_mulu_links=get_mulu_Links()      #获取所有的目录url
    for j in range(0,len(book_mulu_links)):
        time.sleep(0.5)
        url=book_mulu_links.pop()       #获取一个url并对其进行解析
        html = urlopen(url)
        bsObj1 = BeautifulSoup(html, "lxml")
        #使用正则表达式找出所有的图书url
        source = bsObj1.findAll("a",{"href": re.compile("(https:\/\/book\.douban\.com\/subject\/[0-9]{1,}\/)")})
        #将所有的图书url写入bookid这个集合中
        for i in source:
            bookid.add("".join(re.findall(r'subject_id\:\'(\d+)\'',str(i))))
    return bookid

if __name__=='__main__':

    all_bookid=r'.\all_bookids.txt'
    if is_not_empty(all_bookid):
        print "empty"
        bookid = get_book_id()
        from_id_to_txt(all_bookid,'w',bookid)
    else:
        print "pass"
        pass





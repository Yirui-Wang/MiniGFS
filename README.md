# 199行Python代码实现简易Google文件系统

[原文](http://clouddbs.blogspot.com/2010/11/gfs-google-file-system-in-199-lines-of.html)由John Arley在2010年发表在[Python Cloud](http://clouddbs.blogspot.com/)。本项目为Python3代码。

GFS，Google文件系统，是整个Google基础设施的支柱。GFS的简要总结如下。GFS包含三个组件：客户端client，元数据服务器master，和多个数据服务器chunkserver。用户唯一可见的只有客户端。它的功能类似于一个标准POSIX文件库。master是文件系统中保存元数据的服务器，元数据服务器只有1个。元数据指的是每个chunk的信息与其在chunkserver上的位置。为了避免master成为系统的瓶颈，实际数据是存储在chunkserver上的，绝大多数的网络交互也是发生在client和chunkserver之间的。下面我们详细地描述在python中如何实现GFS的client，master，和chunkserver类，并对其进行测试。

GFSClient类是GFS系统中唯一的用户可见的部分。它是文件系统client与master和chunkserver的桥梁，即所有从客户端发出的用于存储或获取文件的请求都需要使用这个类才能与master和chunkserver节点进行交互。需要注意的是，GFS的外表与通常的文件系统非常相似，使用它并不需要分布式的知识，这是因为在client的实现中进行了抽象。当然也有一些例外，比如用于分配最高效的文件处理的局部chunk知识，比如map reduce算法，但在这个实现中，我们避免了这些。最关键的是要注意GFS如何进行正常的read、write、append、exist、和delete调用，以及在client类中是如何实现的；我们也简化了open、close和create。每个函数的要点是一样的：从master获取元数据（包含chunk ID和chunk位置），更新master上的元数据，最后与chunkserver进行实际的数据传输。

```python
class GFSClient:
    def __init__(self, master):
        self.master = master

    def write(self, filename, data):    # filename is full namespace path
        if self.exists(filename):       # if already exists, overwrite
            self.delete(filename)
        num_chunks = self.num_chunks(len(data))
        chunkuuids = self.master.alloc(filename, num_chunks)
        self.write_chunks(chunkuuids, data)

    def write_chunks(self, chunkuuids, data):
        chunks = [data[x: x+self.master.chunksize]
                  for x in range(0, len(data), self.master.chunksize)]
        chunkservers = self.master.get_chunkservers()
        for i in range(0, len(chunkuuids)):  # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunkservers[chunkloc].write(chunkuuid, chunks[i])

    def num_chunks(self, size):
        return (size // self.master.chunksize) \
               + (1 if size % self.master.chunksize > 0 else 0)

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: " + filename)
        num_append_chunks = self.num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename,
                                                     num_append_chunks)
        self.write_chunks(append_chunkuuids, data)

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename):  # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: " + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        chunkservers = self.master.get_chunkservers()
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            chunks.append(chunk)
        data = reduce(lambda x, y: x+y, chunks)  # reassemble in order
        return data

    def delete(self, filename):
        self.master.delete(filename)
```

GFSMaster类模拟GFS元数据服务器。它是整个系统的核心节点，存储了所有的元数据。client首先向master发送请求，当获取元数据后，它们直接与chunkserver进行交互。元数据通常是短而低延迟的，这避免了master成为系统的瓶颈。在本实现中，元数据被为一系列的字典，尽管在实际系统中背后会有文件系统支持。我们简化了通过心跳获取chunkserver的状态、chunkserver authentication和用于高效存储的局部信息。然而为了展示分布式系统的工作方式，我们仍然保留了client绕过master对chunkserver直接进行读写操作。


```python
class GFSMaster:
    def __init__(self):
        self.num_chunkservers = 5
        self.max_chunkservers = 10
        self.max_chunksperfile = 100
        self.chunksize = 10
        self.chunkrobin = 0
        self.filetable = {}     # file to chunk mapping
        self.chunktable = {}    # chunkuuid to chunkloc mapping
        self.chunkservers = {}  # loc id to chunkserver mapping
        self.init_chunkservers()

    def init_chunkservers(self):
        for i in range(0, self.num_chunkservers):
            chunkserver = GFSChunkserver(i)
            self.chunkservers[i] = chunkserver

    def get_chunkservers(self):
        return self.chunkservers

    def alloc(self, filename, num_chunks):  # return ordered chunkuuid list
        chunkuuids = self.alloc_chunks(num_chunks)
        self.filetable[filename] = chunkuuids
        return chunkuuids

    def alloc_chunks(self, num_chunks):
        chunkuuids = []
        for i in range(0, num_chunks):
            chunkuuid = uuid.uuid1()
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = chunkloc
            chunkuuids.append(chunkuuid)
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
        return chunkuuids

    def alloc_append(self, filename, num_append_chunks):  # append chunks
        chunkuuids = self.filetable[filename]
        append_chunkuuids = self.alloc_chunks(num_append_chunks)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def get_chunkloc(self, chunkuuid):
        return self.chunktable[chunkuuid]

    def get_chunkuuids(self, filename):
        return self.filetable[filename]

    def exists(self, filename):
        return True if filename in self.filetable else False

    def delete(self, filename):  # rename for later garbage collection
        chunkuuids = self.filetable[filename]
        del self.filetable[filename]
        timestamp = repr(time.time())
        deleted_filename = "/hidden/deleted/" + timestamp + filename
        self.filetable[deleted_filename] = chunkuuids
        print("deleted file: " + filename + " renamed to " + deleted_filename
              + " ready for gc")

    def dump_metadata(self):
        print("Filetable:")
        for filename, chunkuuids in self.filetable.items():
            print(filename, "with", len(chunkuuids), "chunks")
        print("Chunkservers: ", len(self.chunkservers))
        print("Chunkserver Data:")
        for chunkuuid, chunkloc in \
                sorted(self.chunktable.items(), key=operator.itemgetter(1)):
            chunk = self.chunkservers[chunkloc].read(chunkuuid)
            print(chunkloc, chunkuuid, chunk)
```

GFSChunkserver类是此项目中最小的。它代表在大型数据中心中运行的实际的数据服务器，连接到网络可供master和client访问。在GFS中，chunkserver相对“愚蠢”，因为它们只知道chunk，即文件数据被分解成块。它们不知道整个文件的整体情况，在文件系统中的位置，相关的元数据等。我们将这个类实现为一个简单的本地存储，可以在运行测试代码后查看目录路径“gfs/chunks”。在实际系统中，需要持久存储chunk信息以进行备份。

```python
class GFSChunkserver:
    def __init__(self, chunkloc):
        self.chunkloc = chunkloc
        self.chunktable = {}
        self.local_filesystem_root = "gfs/chunks/" + repr(chunkloc)
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "w") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename

    def read(self, chunkuuid):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "r") as f:
            data = f.read()
        return data

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" + str(chunkuuid) \
                         + '.gfs'
        return local_filename
```

我们使用main函数测试所有客户端的方法，包括异常。我们首先创建一个master对象和client对象，然后写一个文件。此写操作由client以与真实GFS相同的方式执行：首先从master获取元数据，然后将chunk直接写入每个chunkserver。追加的功能与之类似。删除以GFS方式处理，它将文件重命名为隐藏的命名空间，并留待以后进行垃圾回收。dump函数显示元数据内容。需要注意的是，这是一个单线程测试，因为此演示程序不支持并发，尽管可以在元数据周围添加适当的锁。

```python
def main():  # test script for filesystem
    # setup
    master = GFSMaster()
    client = GFSClient(master)

    # test write, exist, read
    print("Writing...")
    client.write("/usr/python/readme.txt",
                 "This file tells you all about python that you ever wanted to know. "
                 + "Not every README is as informative as this one, but we aim to please. "
                 + "Never yet has there been so much information in so little space.\n")
    print("File exists?", client.exists("/usr/python/readme.txt"))
    print("Reading...")
    print(client.read("/usr/python/readme.txt"))

    # test append, read after append
    print("Appending...")
    client.write_append("/usr/python/readme.txt",
                        "I'm a little sentence that just snuck in at the end.\n")
    print("Reading...")
    print(client.read("/usr/python/readme.txt"))

    # test delete
    print("Deleting...")
    client.delete("/usr/python/readme.txt")
    print("File exists?", client.exists("/usr/python/readme.txt"))

    # test exceptions
    print("\nTesting Exceptions...")
    try:
        client.read("/usr/python/readme.txt")
    except Exception as e:
        print("This exception should be thrown:", e)
    try:
        client.write_append("/usr/python/readme.txt", "foo")
    except Exception as e:
        print("This exception should be thrown:", e)

    # show structure of the filesystem
    print("\nMetadata Dump...")
    print(master.dump_metadata())
```

输出如下。特别注意最后的master元数据，可以看到chunk如何以混乱的顺序分布在块服务器上，只能由客户端按元数据指定的顺序重新组装。

```
Writing...
File exists? True
Reading...
This file tells you all about python that you ever wanted to know. Not every README is as informative as this one, but we aim to please. Never yet has there been so much information in so little space.

Appending...
Reading...
This file tells you all about python that you ever wanted to know. Not every README is as informative as this one, but we aim to please. Never yet has there been so much information in so little space.
I'm a little sentence that just snuck in at the end.

Deleting...
deleted file: /usr/python/readme.txt renamed to /hidden/deleted/1547263732.7709315/usr/python/readme.txt ready for gc
File exists? False

Testing Exceptions...
This exception should be thrown: read error, file does not exist: /usr/python/readme.txt
This exception should be thrown: append error, file does not exist: /usr/python/readme.txt

Metadata Dump...
Filetable:
/hidden/deleted/1547263732.7709315/usr/python/readme.txt with 27 chunks
Chunkservers:  5
Chunkserver Data:
0 2f1884c6-161a-11e9-a363-9cb6d0e015c6 This file 
0 2f1977b8-161a-11e9-98f9-9cb6d0e015c6  wanted to
0 2f1977bd-161a-11e9-8ebb-9cb6d0e015c6 e as this 
0 2f1977c2-161a-11e9-b17c-9cb6d0e015c6  there bee
0 2f199580-161a-11e9-a379-9cb6d0e015c6 .

0 2f2292b0-161a-11e9-b8ac-9cb6d0e015c6  at the en
1 2f1977b4-161a-11e9-9003-9cb6d0e015c6 tells you 
1 2f1977b9-161a-11e9-b113-9cb6d0e015c6  know. Not
1 2f1977be-161a-11e9-a9ed-9cb6d0e015c6 one, but w
1 2f1977c3-161a-11e9-b1f5-9cb6d0e015c6 n so much 
1 2f2292ac-161a-11e9-b861-9cb6d0e015c6 I'm a litt
1 2f2292b1-161a-11e9-a2eb-9cb6d0e015c6 d.

2 2f1977b5-161a-11e9-8f08-9cb6d0e015c6 all about 
2 2f1977ba-161a-11e9-934a-9cb6d0e015c6  every REA
2 2f1977bf-161a-11e9-8555-9cb6d0e015c6 e aim to p
2 2f1977c4-161a-11e9-b0a7-9cb6d0e015c6 informatio
2 2f2292ad-161a-11e9-905a-9cb6d0e015c6 le sentenc
3 2f1977b6-161a-11e9-9fe5-9cb6d0e015c6 python tha
3 2f1977bb-161a-11e9-93d3-9cb6d0e015c6 DME is as 
3 2f1977c0-161a-11e9-bac9-9cb6d0e015c6 lease. Nev
3 2f19957e-161a-11e9-9ab5-9cb6d0e015c6 n in so li
3 2f2292ae-161a-11e9-8765-9cb6d0e015c6 e that jus
4 2f1977b7-161a-11e9-9979-9cb6d0e015c6 t you ever
4 2f1977bc-161a-11e9-8b12-9cb6d0e015c6 informativ
4 2f1977c1-161a-11e9-b1a6-9cb6d0e015c6 er yet has
4 2f19957f-161a-11e9-8b54-9cb6d0e015c6 ttle space
4 2f2292af-161a-11e9-9a56-9cb6d0e015c6 t snuck in
```

当然，我们缺乏一些完整的系统所需的GFS的复杂性：元数据加锁，chunk租约，复制，master故障转移，数据本地化，chunkserver心跳，垃圾回收。但是我们在这里展示了GFS的要点，这将帮助您更好地理解基础知识。它也可以作为您在python中探索更详细的分布式文件系统代码的起点。
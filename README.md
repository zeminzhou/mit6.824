### 设计一个高效的索引结构

某机器配置为：CPU 8 cores, MEM 4G, HDD 4T, 该及其上有一个1T的无序数据文件，格式
为(key_size: uint64, key:bytes, value_size: uint64, value: bytes), 其中
1B<=keysize<=1KB，1B<=value_size<=1MB。

设计一个索引结构，使得并发随机读取每一个key-value的代价最小，允许对数据文件做任意
预处理，但是预处理计入到整个读取过程的代价里。

### 思路
1. hashtable

    这种方式对内存大小较高，然而数据文件为1TB，而内存只有4G大小，数据量过大。对于
这个问题，一种解决是将数据分段建立hashtable，将hashtable保存到磁盘。因为在各个hashtable
之间没有联系，在查找时只能遍历所有的hashtable。这样如果hashtable的数据过多，则需要多次
读取磁盘。

2. B-Tree

    对于分段hashtable存在的问题，B-Tree可以很好的解决。所以采用B-Tree的方式建立索引。

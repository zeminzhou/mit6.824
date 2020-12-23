#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#define KEYSIZE     1024
#define PATHSIZE    16      // 文件名的大小
#define M           8191   // B-Tree阶8K
#define POLISHING   4078

#define INDEX_FILE_NAME "index_btree"   // 指向B-Tree root节点的文件名
#define DATA_FILE_NAME  "data"          // 存放数据的文件名 

#define MALLOC_NODE(p, type)    type* p = (type*) malloc(sizeof(type)); memset(p, 0, sizeof(type))
#define FREE_NODE(p)            free(p)

#define OPEN_FILE(file_name, mode)                          fopen(file_name, mode)
#define CLOSE_FILE(fp)                                      fclose(fp)
#define OPEN_FILE_READ(file_name, fp, mode, buf, size)      fp = OPEN_FILE(file_name, mode); fread(buf, size, 1, fp)
#define OPEN_FILE_WRITE(file_name, fp, mode, buf, size)     fp = OPEN_FILE(file_name, mode); fwrite(buf, size, 1, fp)

int BTreeNodeCount; // B-Tree节点计数

// B-Tree里面key对应的value，存放原数据value的长度和在文件中的偏移量
typedef struct {
    uint64_t    len;
    size_t      pos;
} Value;

// "index_btree" 对应的内容，主要存放B-Tree root节点的文件名
typedef struct {
    char     head[PATHSIZE];
    uint16_t nodeNum;
} HeadFile;

// B-Tree 主要数据结构，sizeof(BTreeNode)为4K + 8M + 128K + 128K
typedef struct {
    uint16_t    keyNum;                 // 
    char        parent[PATHSIZE];       // 
    char        polishing[POLISHING];   // 4K(补齐4K)
    char        key[M + 1][KEYSIZE];    // 8M
    char        ptr[M + 1][PATHSIZE];   // 128K
    Value       data[M + 1];            // 128K
} BTreeNode;

// 存放搜索结果
typedef struct {
    char        ptr[PATHSIZE];  // 对应节点的文件名
    uint16_t    i;              // 对应key 的索引 
    uint16_t    tag;            // 标记是否搜索到相应的key
    
    uint64_t    len;            // 对应Value中的len
    size_t      pos;            // 对应Value中的pos
} Result;

// 创建新的文件，写入数据
static void writeNewFile(const char* file, void* buf, size_t size) {
    int fd = open(file, O_RDWR | O_CREAT, 0666);
    write(fd, buf, size);
    close(fd);
}

// 为B-Tree节点 生成文件名
static void nextNodeFileName(char* buf) {
    buf[0] = 'n';
    buf[1] = 'o';
    buf[2] = 'd';
    buf[3] = 'e';

    sprintf(buf+4, "%d", BTreeNodeCount);
    BTreeNodeCount++;
}

// 建立B-Tree节点在内存中的映射
static BTreeNode* MmapBTreeNode(const char* file) {
    int fd = open(file, O_RDWR);
    BTreeNode* nodeBuf = (BTreeNode*)mmap(NULL, sizeof(BTreeNode), PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    return nodeBuf;
}

static void MunmapBTreeNode(BTreeNode* nodeBuf) {
    munmap(nodeBuf, sizeof(BTreeNode));
}

// 在B-Tree节点中查找key
static int Search(BTreeNode* NodeBuf, char* key) {
    uint16_t i = 1;
    while (i <= NodeBuf->keyNum) {
        if (strcmp(key, NodeBuf->key[i]) < 0) {
            break;
        }
        i++;
    }
    return i;
}

// 在B-Tree节点中搜索对应的key
static void SearchBTree(char* file, char* key, Result* r) {
    uint16_t    i = 0;
    uint16_t    found = 0;
    char    nodeFile[PATHSIZE];
    char    parentFile[PATHSIZE];

    strcpy(nodeFile, file);
    memset(parentFile, 0, sizeof(parentFile));

    while (nodeFile[0] != 0 && found == 0) {
        BTreeNode* nodeBuf = MmapBTreeNode(file);
        i = Search(nodeBuf, key);
        if (i <= nodeBuf->keyNum && strcmp(nodeBuf->key[i], key) == 0) {
            found = 1;
            r->len = nodeBuf->data[i].len;
            r->pos = nodeBuf->data[i].pos;
        } else {
            strcpy(parentFile, nodeFile);
            strcpy(nodeFile, nodeBuf->ptr[i - 1]);
        }
        MunmapBTreeNode(nodeBuf);
    }

    if (found == 1) {
        strcpy(r->ptr, nodeFile);
        r->i = i;
        r->tag = 1;
    } else {
        strcpy(r->ptr, parentFile);
        r->i = i;
        r->tag = 0;
    }
}

// 更新"index_tree" 指向新的 root节点
static void UpdateFileHead(char* newHeadFile) {
    FILE* fp;
    HeadFile headBuf;
    OPEN_FILE_READ(INDEX_FILE_NAME, fp, "rb+", &headBuf, sizeof(HeadFile));
    if (newHeadFile != NULL) {
        strcpy(headBuf.head, newHeadFile);
    }
    headBuf.nodeNum++;
    fwrite(&headBuf, sizeof(HeadFile), 1, fp);
    CLOSE_FILE(fp);
}

// 创建一个新的root节点
static void NewRootNode(char* rootFile, char* key, char* ap, Value data) {
    char newRootFileName[PATHSIZE];
    nextNodeFileName(newRootFileName);

    MALLOC_NODE(nodeBuf, BTreeNode);
    nodeBuf->keyNum = 1;
    nodeBuf->data[1] = data;
    strcpy(nodeBuf->ptr[0], rootFile);
    strcpy(nodeBuf->ptr[1], ap);
    strcpy(nodeBuf->key[1], key);
    writeNewFile(newRootFileName, nodeBuf, sizeof(BTreeNode));
    FREE_NODE(nodeBuf);

    nodeBuf = MmapBTreeNode(rootFile);
    strcpy(nodeBuf->parent, newRootFileName);
    MunmapBTreeNode(nodeBuf);

    nodeBuf = MmapBTreeNode(rootFile);
    strcpy(nodeBuf->parent, newRootFileName);
    MunmapBTreeNode(nodeBuf);

    UpdateFileHead(newRootFileName);
}

// 分割B-Tree节点
static void Split(BTreeNode* nodeBuf, char* ap, uint16_t s) {
    uint16_t    i, j;
    uint16_t    n = nodeBuf->keyNum;
    BTreeNode*  apNodeBufChild;

    MALLOC_NODE(apNodeBuf, BTreeNode);
    nextNodeFileName(ap);
    strcpy(apNodeBuf->ptr[0], nodeBuf->ptr[s]);

    for (i = s + 1, j = 1; i <= n; i++, j++) {
        strcpy(apNodeBuf->key[j], nodeBuf->key[i]);
        strcpy(apNodeBuf->ptr[j], nodeBuf->ptr[i]);
        nodeBuf->ptr[i][0] = 0;
        apNodeBuf->data[j].len = nodeBuf->data[i].len;
        apNodeBuf->data[j].pos = nodeBuf->data[i].pos;
    }
    apNodeBuf->keyNum = n - s;
    strcpy(apNodeBuf->parent, nodeBuf->parent);
    writeNewFile(ap, apNodeBuf, sizeof(BTreeNode));

    UpdateFileHead(NULL);
    for (i = 0; i <= n - s; i++) {
        if (apNodeBuf->ptr[i][0] != 0) {
            apNodeBufChild = MmapBTreeNode(apNodeBuf->ptr[i]);
            strcpy(apNodeBufChild->parent, ap);
            MunmapBTreeNode(apNodeBufChild);
        }
    }

    nodeBuf->keyNum = s - 1;
    FREE_NODE(apNodeBuf);
}

// 插入数据到B-Tree对应节点
static void Insert(BTreeNode* nodeBuf, uint16_t i, char* ap, char* key, Value data) {
    uint16_t j;

    for (j = nodeBuf->keyNum; j >= i; j--) {
        strcpy(nodeBuf->key[j + 1], nodeBuf->key[j]);
        strcpy(nodeBuf->ptr[j + 1], nodeBuf->ptr[j]);
        nodeBuf->data[j + 1].len = nodeBuf->data[j].len;
        nodeBuf->data[j + 1].pos = nodeBuf->data[j].pos;
    }
    strcpy(nodeBuf->key[i], key);
    strcpy(nodeBuf->ptr[i], ap);
    nodeBuf->data[i].len = data.len;
    nodeBuf->data[i].pos = data.pos;

    nodeBuf->keyNum++;
}

// 插入数据到B-Tree中
static void InsertBTree(char* rootNodeFile, char* ikey, char* nodeFile, uint16_t i, Value data) {
    uint16_t s              = 0;
    uint16_t finished       = 0;
    uint16_t needNewRoot    = 0;
    char    ap[PATHSIZE];
    char    key[KEYSIZE];

    strcpy(key, ikey);
    memset(ap, 0, sizeof(ap));
    BTreeNode *nodeBuf = MmapBTreeNode(nodeFile);

    while (needNewRoot == 0 && finished == 0) {
        Insert(nodeBuf, i, key, ap, data);
        if (nodeBuf->keyNum < M) {
            MunmapBTreeNode(nodeBuf);
            finished = 1;
        } else {
            s = (M + 1)/2;
            Split(nodeBuf, ap, s);
            strcpy(key, nodeBuf->key[s]);
            data.len = nodeBuf->data[s].len;
            data.pos = nodeBuf->data[s].pos;

            if (nodeBuf->parent[0] != 0) {
                strcpy(nodeFile, nodeBuf->parent);
                MunmapBTreeNode(nodeBuf);
                nodeBuf = MmapBTreeNode(nodeFile);
                i = Search(nodeBuf, key);
            } else {
                needNewRoot = 1;
            }
        }
    }
    if (needNewRoot == 1) {
        NewRootNode(rootNodeFile, key, ap, data);
    }
}

static void InsertData(char* key, uint64_t len, size_t pos) {
    FILE*       fp;
    Value       data;
    Result      r;
    HeadFile    headBuf;

    OPEN_FILE_READ(INDEX_FILE_NAME, fp, "rb+", &headBuf, sizeof(HeadFile));
    SearchBTree(headBuf.head, key, &r);

    data.len = len;
    data.pos = pos;
    InsertBTree(headBuf.head, key, r.ptr, r.i, data);

    CLOSE_FILE(fp);
}

// 创建"index_btree" 文件，保存指向B-Tree的root node
static int createIndexHeadFile() {
    HeadFile headBuf;

    if (access(INDEX_FILE_NAME, 0) != 0) {
        MALLOC_NODE(nodeBuf, BTreeNode);
        nodeBuf->keyNum = 0;
        nextNodeFileName(headBuf.head);
        writeNewFile(headBuf.head, nodeBuf, sizeof(BTreeNode));
        FREE_NODE(nodeBuf);

        headBuf.nodeNum = 1;
        writeNewFile(INDEX_FILE_NAME, &headBuf, sizeof(HeadFile));
        return 0;
    }
    return 1;
}

// 建立B-Tree
void createIndex() {
    if (createIndexHeadFile()) {
        return;
    }

    int     fd;
    size_t  offset = 0;
    size_t  size;
    char*   buf;
    struct  stat statbuf;

    fd = open(DATA_FILE_NAME, O_RDONLY);
    stat(DATA_FILE_NAME, &statbuf);
    size = statbuf.st_size;
    buf = (char*)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);   // 读取数据文件
    while (buf != NULL && offset < size) {
        uint64_t lk;
        uint64_t lv;
        char     key[KEYSIZE];

        lk = atoi(buf + offset);            // 读取key_size
        while (*(buf + offset) != ',') {
            offset++;
        }
        offset++;
        memcpy(key, buf + offset, lk);      // 读取key
        offset += lk + 1;
        lv = atoi(buf + offset);            // 读取value_size
        while (*(buf +offset) != ',') {
            offset++;
        }
        offset++;
        InsertData(key, lv, offset);        // 插入到 B-Tree
        offset += lv;
    }
    munmap(buf, size);
}

char* read(char* key) {
    FILE* fp;
    char* res;
    char* buf;
    int   fd;
    Result      r;
    HeadFile    headBuf;

    OPEN_FILE_READ(INDEX_FILE_NAME, fp, "rb+", &headBuf, sizeof(HeadFile));
    SearchBTree(headBuf.head, key, &r);
    CLOSE_FILE(fp);
    if (r.tag == 0) {
        return NULL;
    }

    res = (char*)malloc(sizeof(char)*r.len);
    fd = open(DATA_FILE_NAME, O_RDONLY);
    buf = (char*)mmap(NULL, r.len, PROT_READ, MAP_PRIVATE, fd, r.pos);   // 读取数据文件
    memcpy(res, buf, r.len);
    munmap(buf, r.len);

    return res;
}

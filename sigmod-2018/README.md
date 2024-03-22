# 优化思路

- [x]  理解项目的总体结构
- [x]  将baseline代码中的materialized模型改成late-materialized模型，即每个算子的输入和输出是关系表中的tuple id，而不是一个新的关系表。
- [x]  多线程处理query。
- [ ]  提前为每一个表的每一列建立索引。担心内存不足，还没有实现。

下面是优化前后的性能比较。表格中的数字代表的是查询的时间，数字越小代表性能越好。

| Dataset | Baseline | Late-Materialized（8线程） |
| --- | --- | --- |
| workloads/public | 118704 | 73641 |
| workloads/small | 478 | 633 |

测试化境配置：

1. 操作系统：Ubuntu 22.03
2. 内存：16GB
## How to run
1. 选择baseline或者优化之后的代码
```shell
git checkout -f main # 优化之后的代码
git checkout -f baseline # baseline代码
```
2. 运行测试
```shell
# 测试性能
git checkout -f main
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DFORCE_TESTS=OFF .. # 不编译gtest测试用例
make -j 8
cd ..
bash ./run_test_harness.sh ./workloads/public
```
## 项目总体概况

这个项目是一个数据库优化竞赛。主办方提供了一个baseline代码。选手需要对代码进行优化，使得数据库可以以更高的效率执行join运算。

项目有下面的几个文件需要理解：

1. run.sh：这是一个shell脚本，用于运行数据库本体。
2. run_test_harness.sh：这也是一个shell脚本，用于运行测试。项目代码主要分为数据库代码和测试代码。run_test_harness.sh运行harness.cpp文件中的测试代码，用于测试数据库的性能。harness.cpp中的代码会使用fork+exec的方法运行数据库本体。

# 项目baseline代码分析

## relation类

在baseline代码中，最底层的便是relation类。relation类有几个重要的成员，分别是：

1. std::vector<uint64_t *> columns_;
2. void loadRelation(const char *file_name);

loadRelation函数的作用是从文件中加载关系表。baseline代码中使用的mmap的方式读取文件。columns_存储的关系表的每一列的地址。

**改进**：建立一个buffer pool类来管理页面

## parser.h

parser.h文件中有三个结构体和一个类，负责存储查询语句中的信息。

### 输入的查询语句的规则

下面是一个输入的查询语句的实例：

```
9 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3 0.4
```

一个查询语句由两个“|”分成三个部分。第一个部分是需要用到的关系的列表。上面的“9 0 2”是指，本条查询需要用到r9、r0和r2关系。第二个部分是谓词部分，用于指定filer和join的条件。比如，”0.1=1.0“表示第一部分的关系列表中的第0个关系表(r9)的第1列与第1个关系表(r0)的第0列相同，这是一个join的条件；又比如，“0.0>12472”表示第1个关系表(r0)的第0列需要大于12472，这是一个filer条件。最后一个部分指定需要投影的列。比如“1.0”表示第1个关系表(r0)的第0列。

### SelectInfo

一个结构体，负责表示查询语句上针对一个表的某一列的操作，包含三个重要成员：

1. rel_id：关系的id
2. col_id: 列的id
3. binding：在查询语句的谓词部分，我们不直接使用关系表的真实名称，而是使用关系表的binding。具体请看[这里](https://www.notion.so/sigmoid-2018-aeba8445291a4569b39a6b3b309cc1c4?pvs=21)。

### FilterInfo

关于where语句的信息。主要包含下面三个成员：

1. SelectInfo filter_column：说明了针对哪个表的那一列做筛选
2. Comparison comparison：做筛选时的比较运算符
3. uint64_t constant：常量

比如, where a.c > 1，其中comparison就是a.c，constant就是1，comparison就是Greater

### PredicateInfo

记录了join运算应该在哪两个表的哪两列上进行。

1. SelectInfo left;
2. SelectInfo right;

a join b with a.c = b.d，就有a.c就是left，b.d就是right。

### QueryInfo类

集合了SelectInfo、FilterInfo、PredicateInfo的信息。

## Operator类

bool require(SelectInfo info)函数：告诉Operator，info指向的这一列需要用到。最开始从checksum.run函数里被调用。checksum调用join算子的require函数，告诉require需要用到这些列。join算子又告诉scan算子需要用到这些列。

std::unordered_map<SelectInfo, unsigned> select_to_result_col_id_; 记录selectinfo对应的列放在tmp_result的哪一个位置。

unsigned resolve(SelectInfo info)

```
    // info中的binding确定唯一的关系表，col_id确定唯一的列
    // select_to_result_col_id_中存储的是列在result_columns_中的位置
```

void run();

std::vector<std::vector<uint64_t>> tmp_results_ 存储物化关系表的缓冲池。join算子会把生成的新关系存储在tmp_results中

### Scan类

relation

relation_binding

### FilterScan类

std::vector<uint64_t *> input_data_;  存储关系表中需要用到的列？

void copy2Result(uint64_t id);

## joiner类

joiner是项目中负责执行

analyzeInputOfJoin

改进：修改join算法中的HT的用法

# 改进

### 一次性将所有的表读入内存

### 排序

很难做，因为不知道根据哪一个键排序

### 建立索引 && late-materialized机制

1. 多线程建立索引

如果使用物化模型，则会导致join算子返回的关系表都是没有索引的新表，无法使用索引提高join的速度。

### 多线程处理查询

### 不实用全物化机制

### 在使用完一个算子之后可以立刻释放算子拥有的资源

## 改进进度

### 3月20日

在每一个operator中加入了context类记录执行的上下文信息，以解决下面的两个问题：

1. 在late materilized模型中，operator只知道tuple id但是无法获取真实数据的问题
2. 无法知道当前的query需要使用多少个关系表的问题

### 3月21日

将算子的输入修改为：vector<vecotr<TupleId>> input_data;

将算子的输出修改为：vector<vector<TupleId>> tmp_data;

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/675ae1e4-871f-4d19-8233-b1315986757a/a66cb20c-2b73-438a-9b17-b82ff835a392/Untitled.jpeg)

因为在baseline代码中，每一个算子都会生成一个全新的表，所以必须使用select_to_result_col_id_记录新生成的表的列与原本的关系表的列之间的关系。但是在我们实现的late-materialized方法中，算子的结果只包括每个表的tuple id，所以不需要记录新表的列和旧表的列之间的映射关系。所以也就不需要select_to_result_col_id_变量。

同样的道理，在baseline代码中，父算子需要调用子算子的require函数，以告知子算子应该把原关系表中的哪一列放入到新生成的关系表中。但是在我们的要实现的late-materialized方法中，算子的结果是每一个表的tuple id，所以我们不需要使用require函数指定需要使用原关系表的哪一列。

综上，我们需要删除select_to_result_col_id和require函数。

将materialized转成了late-materialized模型之后，在public数据集上的查询时间优化到110215，而原本的时间是344683。

很奇怪，昨天在public数据集上测试的结果明明是344683，但是刚才测试了一下昨天的方法，发现时间降到了87708。百思不得其解，难道说我优化了个寂寞？

要不关机重启之后重新测试？

下一步优化：多线程

已完成多线程优化

### 3月22日

刚才看了一loadRelation函数的代码，突然发现在我写完了late-materialized之后，loadrelation函数的实现方式仍然是mmap。 😅
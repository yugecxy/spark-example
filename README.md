# spark-example
spark的编程样例

### hadoop native library 的配置
背景： spark读取snappy结尾的文件需要重新导入hadoop支持
使用：
  1.使用 brew 安装 snappy $ brew install snappy
  2.将lib下面的hadoop_native文件夹的内容加到classpath


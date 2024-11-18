//
// Created by swx on 23-12-28.
//
#include <iostream>
#include "raft.h"
// #include "kvServer.h"
#include <kvServer.h>
#include <unistd.h>
#include <iostream>
#include <random>

void ShowArgsHelp();

int main(int argc, char **argv) {
  //////////////////////////////////读取命令参数：节点数量、写入raft节点节点信息到哪个文件
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }
  int c = 0;
  int nodeNum = 0;
  std::string configFileName;
  std::random_device rd;   // 随机数生成器，通常用于获取系统级的随机源
  std::mt19937 gen(rd());  // std::mt19937 是一种伪随机数生成器，基于梅森旋转算法（Mersenne Twister），可以生成高质量的伪随机数。
  std::uniform_int_distribution<> dis(10000, 29999);   // 表示生成一个介于 10000 和 29999 之间（包括 10000 和 29999）的随机整数。
  unsigned short startPort = dis(gen);  // 生成的随机端口号。
  while ((c = getopt(argc, argv, "n:f:")) != -1) {
    switch (c) {
      case 'n':
        nodeNum = atoi(optarg);
        break;
      case 'f':
        configFileName = optarg;
        break;
      default:
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
  }
  std::ofstream file(configFileName, std::ios::out | std::ios::app);
  file.close();
  // 以清空文件的方式打开文件
  std::ofstream file2(configFileName, std::ios::out | std::ios::trunc); 
  if (file2.is_open()) {
    file2.close();
    std::cout << configFileName << " 已清空" << std::endl;
  } else {
    std::cout << "无法打开 " << configFileName << std::endl;
    exit(EXIT_FAILURE);
  }
  // 创建多个节点
  for (int i = 0; i < nodeNum; i++) {
    short port = startPort + static_cast<short>(i);
    std::cout << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid() << std::endl;
    pid_t pid = fork();  // 创建新进程
    if (pid == 0) {
      // 如果是子进程
      // 子进程的代码

      auto kvServer = new KvServer(i, 500, configFileName, port);
      //pause() 会使子进程进入等待状态，直到收到一个信号为止。这样，子进程不会退出，而是保持运行，等待进一步的操作
      pause();  // 子进程进入等待状态，不会执行 return 语句
    } else if (pid > 0) {
      // 如果是父进程
      // 父进程的代码
      sleep(1);
    } else {
      // 如果创建进程失败
      std::cerr << "Failed to create child process." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  pause();
  return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }

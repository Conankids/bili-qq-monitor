# bili-qq-monitor

一个面向 B 站评论区的实时轮询监控服务。

它会持续扫描你指定视频的评论区，识别两类评论者：

1. 视频 `UP 主本人`
2. 你额外指定的评论用户

命中后，服务会通过 QQ 官方机器人把提醒推送到群聊或私聊。

## 能力概览

- 监控指定 `BV` 视频的评论区
- 监听 `UP 主评论`
- 监听指定用户评论
  当前支持按昵称精确匹配，也支持按 UID 兜底
- 支持群聊和私聊两种机器人配置入口
- 支持私聊 QQ 主动提醒
- 支持连续失败告警
  单个 `BV` 连续失败达到阈值后，自动私聊提醒
- 支持运行时命令
  不需要改代码就可以新增、移除或清空监控

## 当前实现

- QQ 侧：`AccessToken + OpenAPI + Gateway WebSocket`
- B 站侧：公开评论接口轮询
- 存储：本地 `state.json`

顶层评论当前按“最新评论”排序扫描，优先发现新评论。

## 已知限制

### 1. QQ 主动推送策略受官方限制

按 QQ 官方文档 2026-03-05 的说明，主动推送能力可能被限制或拒绝。  
也就是说，服务端逻辑可以正常命中评论，但 QQ 平台不一定保证每种场景都允许主动发消息。

### 2. 这不是 B 站官方事件订阅

本项目本质上是“近实时轮询”，不是 webhook。  
轮询频率越高，实时性越好，但越容易触发 B 站风控。

### 3. 昵称监听不是强标识

昵称匹配采用“精确匹配”。

- 好处：简单，误报少
- 风险：昵称可能重名，也可能改名

如果你需要更稳，优先用 UID：

```text
/watchuid 279321940
```

## 环境要求

- Python 3.11+
- 已创建好的 QQ 官方机器人
- 机器人具备对应的群聊或私聊能力

## 快速开始

### 1. 安装依赖

```bash
cd /root/bili_qq_monitor_official
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 2. 配置 `.env`

至少填写：

```env
QQ_APPID=你的APPID
QQ_APP_SECRET=你的机器人密钥
```

常用可选项：

```env
BILI_BVIDS=BV16jcyzVEUs
BILI_WATCH_USER_NAMES=萧风明洛
BILI_POLL_INTERVAL=10
BILI_FAILURE_NOTIFY_THRESHOLD=3
```

### 3. 启动服务

```bash
cd /root/bili_qq_monitor_official
source .venv/bin/activate
python main.py
```

## 命令说明

机器人支持在群里 `@机器人` 或直接私聊使用命令。

### 视频监控

```text
/watch BV号
```

添加一个监控视频。

```text
/unwatch BV号
```

移除一个监控视频。

```text
/clearwatch
```

清空所有监控视频。

### 评论用户监控

```text
/watchuser 昵称
```

按昵称精确匹配监听评论用户。

```text
/unwatchuser 昵称
```

移除昵称监听。

```text
/watchuid UID
```

按 UID 监听评论用户。

```text
/unwatchuid UID
```

移除 UID 监听。

### 状态与帮助

```text
/status
```

查看当前监控状态。

```text
/h
/help
```

查看完整命令帮助。

## 环境变量

常用环境变量如下：

```env
QQ_APPID=
QQ_APP_SECRET=

TARGET_GROUP_OPENIDS=
TARGET_USER_OPENIDS=

BILI_BVIDS=
BILI_WATCH_USER_MIDS=
BILI_WATCH_USER_NAMES=
BILI_SESSDATA=

BILI_POLL_INTERVAL=10
BILI_MAX_PAGES=10
BILI_FAILURE_NOTIFY_THRESHOLD=3

STATE_PATH=./state.json
```

说明：

- `TARGET_GROUP_OPENIDS`
  预置群目标
- `TARGET_USER_OPENIDS`
  预置私聊目标
- `BILI_BVIDS`
  启动时预置监控视频
- `BILI_WATCH_USER_MIDS`
  启动时预置评论用户 UID
- `BILI_WATCH_USER_NAMES`
  启动时预置评论用户昵称
- `BILI_SESSDATA`
  可选的 B 站登录态。高频扫描或子评论抓取时建议提供
- `BILI_POLL_INTERVAL`
  轮询间隔，单位秒
- `BILI_MAX_PAGES`
  每轮扫描顶层评论的最大页数
- `BILI_FAILURE_NOTIFY_THRESHOLD`
  单个 `BV` 连续失败多少次后发一条私聊告警

## 状态文件

默认状态文件是：

```text
./state.json
```

它会记录：

- 已登记的群 `openid`
- 已登记的私聊用户 `openid`
- 当前监控视频列表
- 当前监听的评论 UID
- 当前监听的评论昵称
- 已推送过的评论 `rpid`
- 已处理过的 QQ 事件 ID

## 常见问题

### 为什么评论已经发了，但没有触发提醒？

常见原因有这些：

- 该评论还没有被 B 站公开接口返回
- 评论在旧楼层子回复里，且该楼层接口短时触发了风控
- 昵称不完全一致
- QQ 主动推送被平台限制

优先排查 `service.log`。

### 为什么建议配置 `SESSDATA`？

高频扫描和子评论抓取更容易触发 `412` 或 `-352`。  
提供登录态后，接口稳定性通常会更好。

### 连续失败提醒会不会刷屏？

不会。

- 单个 `BV` 达到阈值时只提醒一次
- 后续恢复成功会自动清零
- 恢复前不会重复发送同类告警

## 安全建议

- 不要把 `.env`、`SESSDATA`、运行日志和 `state.json` 提交到公开仓库
- 如果你曾在聊天、工单或截图里暴露过 QQ 机器人密钥或 B 站登录态，建议尽快轮换

## 开源协议

本项目使用 [MIT License](./LICENSE)。

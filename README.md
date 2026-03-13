# B 站 UP 评论监控 -> QQ 官方机器人

这是一个常驻 Python 服务，做两件事：

1. 通过 B 站公开评论接口轮询指定视频的评论区。
2. 识别 `member.mid == owner.mid` 的评论，也就是 UP 主本人发的评论。
3. 额外识别你指定的 B 站用户 `mid/uid` 或昵称发的评论。
4. 命中后尝试通过 QQ 官方机器人发群提醒或私聊提醒。

顶层评论扫描当前使用“最新评论”排序，以优先保证新评论能尽快被发现。

## 先看限制

按 QQ 官方文档 `发送消息` 页面 2026-03-05 的说明，`主动推送能力于 2025-04-21 起不再提供，接口调用时会收到错误信息`。  
因此这套程序会把官方鉴权、网关事件接入、群消息发送都实现完整，但你这个核心动作:

- `B站轮询发现新评论 -> 机器人主动往 QQ 群发消息`

在当前官方策略下可能直接被平台拒绝。  
如果日志里出现相关错误，问题不在代码，而在平台能力本身。

## 方案结构

- QQ 侧：用官方 `AccessToken + OpenAPI + Gateway WebSocket`
- B 站侧：轮询评论接口
- 状态持久化：本地 `state.json`

程序会自动处理这些事情：

- 维护 QQ `access_token`
- 连接 QQ `gateway` 并保持心跳
- 接收群相关事件
- 接收私聊相关事件
- 记录 `group_openid`
- 记录用户 `openid`
- 支持在群里 `@机器人 /watch BV号` 动态添加监控
- 支持私聊机器人发送 `/watch BV号` 动态添加监控
- 支持 `/clearwatch` 清空所有监控视频
- 支持在群里或私聊里发送 `/watchuser 昵称` 添加“指定评论用户”
- 支持在群里或私聊里发送 `/unwatchuser 昵称` 移除“指定评论用户”
- 仍支持 `/watchuid UID` 和 `/unwatchuid UID` 作为兜底
- 支持 `@机器人 /status` 查看状态
- 支持 `/h` 或 `/help` 查看全部命令和用法
- 支持私聊发送 `/status` 查看状态
- 发现 UP 评论后，尝试调用 `/v2/groups/{group_openid}/messages`
- 发现 UP 评论后，尝试调用 `/v2/users/{openid}/messages`
- 单个 BV 轮询连续失败达到阈值后，自动私聊发送异常提醒

默认推送内容里只带 `BV号`，不直接发 URL。  
这是为了避开官方文档里“消息内容包含 URL 需要先在后台配置消息 URL”的限制。

## 运行要求

- Python 3.11+
- 一个已创建好的 QQ 官方机器人
- 机器人已开通你需要的群能力

## 安装

```bash
cd /root/bili_qq_monitor_official
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

然后编辑 `.env`：

```env
QQ_APPID=你的APPID
QQ_APP_SECRET=你的机器人密钥
BILI_BVIDS=BV1xx411c7mD
```

## 启动

```bash
cd /root/bili_qq_monitor_official
source .venv/bin/activate
python main.py
```

## 使用方式

### 方式 1：在群里动态配置

把机器人拉进群后，群里 `@机器人` 发送：

```text
/watch BV1xx411c7mD
```

查看状态：

```text
/status
```

查看帮助：

```text
/h
```

移除监控：

```text
/unwatch BV1xx411c7mD
```

清空所有监控视频：

```text
/clearwatch
```

### 方式 2：私聊机器人动态配置

直接给机器人私聊发送：

```text
/watch BV1xx411c7mD
```

添加指定评论用户：

```text
/watchuser 某个昵称
```

查看状态：

```text
/status
```

查看帮助：

```text
/h
```

移除监控：

```text
/unwatch BV1xx411c7mD
```

清空所有监控视频：

```text
/clearwatch
```

移除指定评论用户：

```text
/unwatchuser 某个昵称
```

如果昵称重名、改名，或者你想更稳一点，也可以继续用 UID：

```text
/watchuid 279321940
/unwatchuid 279321940
```

### 方式 3：环境变量预置

如果你已经知道目标群的 `group_openid`，可以在 `.env` 写：

```env
TARGET_GROUP_OPENIDS=群openid1,群openid2
```

如果你已经知道目标用户的 `openid`，也可以在 `.env` 写：

```env
TARGET_USER_OPENIDS=用户openid1,用户openid2
```

如果你想开机就监听某些 B 站用户的评论，也可以在 `.env` 写：

```env
BILI_WATCH_USER_MIDS=279321940,343464917
BILI_WATCH_USER_NAMES=某个昵称,另一个昵称
BILI_FAILURE_NOTIFY_THRESHOLD=3
```

## 状态文件

默认会在当前目录生成 `state.json`，保存：

- 已记录的 `group_openid`
- 已记录的用户 `openid`
- 当前监控的视频列表
- 当前监听的 B 站评论用户 `mid`
- 当前监听的 B 站评论用户昵称
- 已推送过的评论 `rpid`
- 已处理过的 QQ 事件 ID

## 昵称匹配说明

昵称监听采用“精确匹配”。

- 优点：实现简单，误报更少
- 缺点：B 站昵称不是唯一标识，且用户可以改名

如果你遇到以下情况，建议改用 UID：

- 同名用户很多
- 用户频繁改昵称
- 你要求误报尽量低

## 常见问题

### 1. 为什么已经检测到 UP 评论，但 QQ 群里没有提醒？

最常见原因有两个：

- 当前没有有效的 `group_openid`
- QQ 官方已拒绝主动推送

先看日志里有没有类似错误响应。

### 2. 为什么不用 Webhook？

官方文档里很多群事件当前仍标成 `WebSocket` 推送，这个实现直接按网关模式接入，少一层 HTTPS 回调配置。

### 3. B 站会不会漏？

这套方案是近实时轮询，不是 B 站官方事件订阅。  
对评论很多的视频，建议把 `BILI_MAX_PAGES` 提高一些，代价是请求更多。

### 4. 查询接口连续失败会怎么样？

程序会按单个 `BV` 统计连续失败次数。

- 默认连续失败 `3` 次后，给已登记的私聊 QQ 目标发一条异常提醒
- 恢复成功后，失败计数自动清零
- 恢复前不会重复刷屏

## 你现在最该做的事

你在聊天里已经贴出了机器人密钥。这个密钥现在应该视为已泄露。  
建议先去 QQ 开放平台把机器人密钥轮换掉，然后再把新密钥写入 `.env`。

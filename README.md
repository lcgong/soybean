**Soybean**是一款轻量型面向消息的业务应用框架，支持RocketMQ系统。

# 1. 核心思路

**Soybean**框架逻辑非常的简单：执行某个业务动作(action)，会产生一条消息(message)，立即或随后，某个或多个反应器(reactor)见到这个消息就会做出反应(react)执行某些逻辑或业务动作。

消息是连接“动作”和“反应器”的媒介。面向消息业务应用设计和业务逻辑实现的关键，就是业务消息的设计，即消息类型结构和消息内容结构设计，消息类型可以在领域(domain)，主题(topic)和标签(tag)三个层级类型进行划分，消息内容结构一般采用类似JSON结构。


# 2. 使用介绍

## 2.1 定义领域(domain)与主题(topic)

定义名为soybean_samples的领域，采用RocketMQ作为消息信道，
信道注册服务地址`localhost:9876`。在领域内定义名为`OddEvenComputation`的主题。
```py
import soybean

domain = soybean.RocketMQ("soybean_samples", "localhost:9876")

topic = domain.topic("OddEvenComputation")
```

## 2.1 动作（action）

定义动作
```py
@topic.action("Result")
async def compute_at_even_step(step_no, result):

    result["value"] = result["step"] * 2
    print(f"{'step_even':>10s}: {result}")
    return result
```

执行动作。执行业务动作会运行其函数并将return的值作为消息内容，以指定的标签和主题发送，直到发送成功。
```py
result["value"] = ...
await compute_at_even_step(step_no, result)
```

## 2.2 事务性动作（transactional action）

如果action的函数是一个事务性的函数，则完成action之后所发送的消息是事务性消息，
以保证事务和所发送的消息是一致性的，事务和消息发送要么最终都以成功，要么都失败。

```py
@order_topic.action("Commit")
@dbconn.transaction
async def order_commit_action(order):
    ....
    await dbconn.sql("UPDATE ....")
    order["status"] = "committed"
    return { }
```

## 2.3 反应器(reactor)

反应器是一个使用`.react`装饰的异步函数，没有返回值。

```py
@topic.react("Result")
async def on_result(message_id, message):
    ....

```

函数输入参数，可以使用下面参数定义
* message 消息内容
* message_id 消息ID
* message_keys 
* message_tags 消息标签
* message_topic 消息主题



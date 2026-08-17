# 频道标签 & 目录归类方案

## 1. 现状分析

### 1.1 下载目录结构

容器内下载路径：`/app/downloads/`（宿主机 `/volume1/docker/hermes-telegram-downloader/downloads/`）

config.yaml 当前配置：
```yaml
save_path: /app/downloads
file_path_prefix:
  - chat_title
  - media_datetime
```

路径生成逻辑（`app.py:get_file_save_path`）：`/app/downloads/{chat_title}/{media_datetime}/{filename}`

当前 downloads 目录为空（容器重启后数据在 `/volume6` 挂载点）。实际下载文件分布在 NAS 的 `/volume6/` 下多个零散目录。

### 1.2 NAS 现有目录（/10t/ 挂载点下的频道目录）

从 Emby DB 查到的 type=3（文件夹）目录：

| 目录名 | 内容类型 |
|--------|---------|
| `onlyfans推特分享` | OnlyFans/Twitter 素人 |
| `欧美AV高清无码中文字幕` | 欧美 AV 中文字幕 |
| `欧美` | 欧美普通 |
| `欧美精品资源` | 欧美精品 |
| `欧美精品💋媚黑母狗` | 欧美媚黑 |
| `欧美资源交流` | 欧美资源 |
| `欧美近身肉搏小屋💋` | 欧美 |
| `欧美高清无码AV` | 欧美无码 |
| `🔍搬运工｜异世界福利` | 欧美搬运 |
| `US+EU` | 欧美 |
| `高清无码中字欧美AV【阿沐】` | 欧美无码 |
| `捷克欧美精选无码Av黑人白人` | 欧美无码 |
| `国产糖心反差` | 国产糖心 |
| `精品AV国产自拍共享` | 国产自拍 |
| `🔞射射屋_国产_糖心_吃瓜_91_杏吧_萝莉` | 国产合集 |
| `💋91资源全集 国产传媒 自拍剧情🈲️` | 91国产 |
| `绅士仓库『NSFW』` | 混合 |
| `👅福利姬｜Cosplay ｜卡哇伊好想舔👅` | 福利姬/Cosplay |
| `丝袜高跟巨乳美乳精品👠` | 丝袜福利 |
| `花园•ᴗ•萝莉少女熟女欧美 [NSFW]` | 混合 |
| `马里奥小屋` | 混合 |
| `organized_strm` | Emby strm 结构化目录 |

### 1.3 Emby 媒体库

Emby 实例1（organized_strm）：`/volume1/cloudNas/ed2k strm/organized_strm/<演员名>/<番号>/`
Emby 实例2（xiaoya-emby）：`/volume6/xiaoya-emby/config/`，DB `library.db`

Emby 媒体库结构（从 DB type=3 文件夹推断）：
- `/media/` — 纪录片、动漫、电视剧、电影、音乐等正规媒体
- `/10t/` — 成人内容，按频道目录分散

### 1.4 代码中的路径生成

```python
# app.py:739-769
def get_file_save_path(self, media_type, chat_title, media_datetime):
    res = self.save_path  # /app/downloads
    for prefix in self.file_path_prefix:  # ["chat_title", "media_datetime"]
        if prefix == "chat_title":
            res = os.path.join(res, chat_title)
        elif prefix == "media_datetime":
            res = os.path.join(res, media_datetime)
    return res
```

`chat_title` 来自 TG 频道标题，`media_datetime` 来自消息日期。

### 1.5 Bot 命令体系

当前注册的命令（`bot.py:_register_bot_handlers`）：

| 命令 | 功能 |
|------|------|
| `/download` | 下载指定频道 |
| `/forward` | 转发消息 |
| `/listen_forward` | 监听频道转发 |
| `/help` | 帮助 |
| `/get_info` | 获取信息 |
| `/start` | 启动 |
| `/set_language` | 设置语言 |
| `/add_filter` | 添加过滤器 |
| `/add_ad` | 添加广告过滤 |
| 媒体转发 | 自动下载（`download_forward_media`）|
| t.me 链接 | 自动下载（`download_from_link`）|

Handler 注册方式：`self.bot.add_handler(MessageHandler(callback, filters=...))`

## 2. 目标

1. 在 `downloads/` 下创建 4 个大目录（4 个媒体库分类）
2. 把现有零散目录归类到 4 个大目录中
3. 给每个频道打标签（映射到 4 个分类之一）
4. 下载时根据频道标签自动放入对应目录
5. 新频道首次下载时，通过 TG 弹出 Inline Keyboard 选择归类
6. TG bot 新增指令支持修改频道标签

## 3. 方案设计

### 3.1 四个媒体库分类

根据现有目录内容和 Emby 媒体库结构，建议 4 个分类：

| 标签 ID | 目录名 | 包含内容 | 对应现有目录示例 |
|---------|--------|---------|-----------------|
| `jp_av` | `日本AV` | 日本 AV（有码/无码/中文字幕） | `organized_strm/` 下的演员目录 |
| `western` | `欧美` | 欧美 AV、OnlyFans、Twitter 素人 | `onlyfans推特分享`, `欧美AV高清无码中文字幕`, `欧美`, `US+EU` 等 |
| `domestic` | `国产` | 国产自拍、糖心、91、麻豆 | `国产糖心反差`, `精品AV国产自拍共享`, `🔞射射屋`, `💋91资源全集` |
| `special` | `特色` | 福利姬、Cosplay、丝袜、绅士仓库等 | `👅福利姬`, `丝袜高跟巨乳美乳精品👠`, `绅士仓库`, `花园•ᴗ•` |

### 3.2 目录结构

```
downloads/
├── 日本AV/          # jp_av
│   ├── {chat_title}/
│   │   └── {media_datetime}/
│   │       └── {filename}
├── 欧美/            # western
│   ├── {chat_title}/
│   │   └── {media_datetime}/
│   │       └── {filename}
├── 国产/            # domestic
│   ├── {chat_title}/
│   │   └── {media_datetime}/
│   │       └── {filename}
└── 特色/            # special
    ├── {chat_title}/
    │   └── {media_datetime}/
    │       └── {filename}
```

保留 `file_path_prefix: [chat_title, media_datetime]` 不变，只是在外层加一个分类目录。

### 3.3 频道标签存储

新建配置文件 `channel_tags.yaml`（和 config.yaml 同级）：

```yaml
# chat_id → tag_id
"-1001521978999": "western"      # onlyfans推特分享
"-1002057294167": "western"      # 欧美AV高清无码中文字幕
"-1003701127803": "domestic"     # 国产糖心反差
"-1002533442302": "special"      # 特色频道
# ... 等等
```

标签定义放在 config.yaml 中：

```yaml
channel_tags:
  jp_av: "日本AV"
  western: "欧美"
  domestic: "国产"
  special: "特色"
default_tag: "special"  # 未设置标签时的默认归类
```

### 3.4 代码改动

#### 3.4.1 路径生成（`module/app.py`）

修改 `get_file_save_path`，在外层插入分类目录：

```python
def get_file_save_path(self, media_type, chat_title, media_datetime, chat_id=None):
    res = self.save_path
    # 插入频道标签目录
    if chat_id is not None:
        tag = get_channel_tag(chat_id)  # 新函数
        if tag:
            tag_dir = self.channel_tags.get(tag, "未分类")
            res = os.path.join(res, tag_dir)
    for prefix in self.file_path_prefix:
        if prefix == "chat_title":
            res = os.path.join(res, chat_title)
        elif prefix == "media_datetime":
            res = os.path.join(res, media_datetime)
    return res
```

#### 3.4.2 新增 `module/channel_tag.py`

```python
import yaml, os, logging

logger = logging.getLogger(__name__)
_tags_config = {}
_channel_tags = {}
_tags_file = None

def init_channel_tags(config, config_dir):
    """初始化频道标签配置"""
    global _tags_config, _channel_tags, _tags_file
    _tags_config = config.get("channel_tags", {
        "jp_av": "日本AV",
        "western": "欧美",
        "domestic": "国产",
        "special": "特色",
    })
    _tags_file = os.path.join(config_dir, "channel_tags.yaml")
    if os.path.exists(_tags_file):
        with open(_tags_file, "r", encoding="utf-8") as f:
            _channel_tags = yaml.safe_load(f) or {}

def get_channel_tag(chat_id) -> str:
    """获取频道的标签ID，未设置返回 default_tag"""
    cid = str(chat_id)
    if cid in _channel_tags:
        return _channel_tags[cid]
    return None  # 未设置

def get_tag_dir_name(tag_id) -> str:
    """获取标签对应的目录名"""
    return _tags_config.get(tag_id, "未分类")

def set_channel_tag(chat_id, tag_id):
    """设置频道标签"""
    _channel_tags[str(chat_id)] = tag_id
    save()

def save():
    """持久化"""
    with open(_tags_file, "w", encoding="utf-8") as f:
        yaml.dump(_channel_tags, f, allow_unicode=True)

def get_all_tags():
    """获取所有标签定义"""
    return _tags_config

def get_all_channel_tags():
    """获取所有频道标签映射"""
    return _channel_tags
```

#### 3.4.3 传递 chat_id 到路径生成

在 `media_downloader.py` 的 `download_media` 函数中，`get_file_save_path` 调用处传入 `node.chat_id`：

```python
# media_downloader.py 中 _get_media_meta 调用 get_file_save_path 的地方
file_save_path = app.get_file_save_path(
    media_type, chat_title, media_datetime,
    chat_id=node.chat_id  # 新增
)
```

#### 3.4.4 新频道首次下载弹出选择

在 `download_forward_media` handler（bot.py:1355）和 `_consume_one_pending`（bot.py:1962）中，下载前检查频道是否已设置标签：

```python
from module.channel_tag import get_channel_tag, get_all_tags

# 在 direct_download 或 _consume_one_pending 中
tag = get_channel_tag(chat_id)
if not tag:
    # 新频道，弹出 Inline Keyboard 让用户选择
    tags = get_all_tags()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(tag_name, callback_data=f"settag:{chat_id}:{tag_id}")]
        for tag_id, tag_name in tags.items()
    ])
    await bot.send_message(
        user_id,
        f"📡 新频道检测：{chat_title}\n请选择该频道的归类：",
        reply_markup=keyboard
    )
    # 任务暂存为 pending，等用户选择后再消费
    return
```

注册 CallbackQueryHandler 处理按钮回调：

```python
# bot.py: _register_bot_handlers 中新增
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

self.bot.add_handler(
    CallbackQueryHandler(
        handle_tag_selection,
        filters=pyrogram.filters.regex(r"^settag:")
    )
)

async def handle_tag_selection(client, callback_query):
    """处理频道标签选择回调"""
    data = callback_query.data.split(":")  # settag:{chat_id}:{tag_id}
    chat_id = data[1]
    tag_id = data[2]
    set_channel_tag(chat_id, tag_id)
    tag_name = get_tag_dir_name(tag_id)
    await callback_query.answer(f"已归类到: {tag_name}")
    await callback_query.message.edit_text(
        f"✅ 频道已归类到「{tag_name}」\n下载将继续..."
    )
    # 触发 pending consumer 继续消费
    from module.bot import _bot
    _bot.app.loop.create_task(_consume_one_pending())
```

#### 3.4.5 新增 bot 指令

**`/set_tag` — 修改频道标签**

```
/set_tag <chat_id> <tag_id>
/set_tag -1001521978999 western
```

```python
async def set_tag_command(client, message):
    """设置或修改频道标签"""
    args = message.text.split()
    if len(args) != 3:
        tags = get_all_tags()
        tag_list = "\n".join([f"  {tid}: {tname}" for tid, tname in tags.items()])
        await message.reply(
            f"用法: /set_tag <chat_id> <tag_id>\n\n"
            f"可用标签:\n{tag_list}\n\n"
            f"查看已设置的频道: /list_tags"
        )
        return
    chat_id = args[1]
    tag_id = args[2]
    if tag_id not in get_all_tags():
        await message.reply(f"❌ 未知标签: {tag_id}")
        return
    set_channel_tag(chat_id, tag_id)
    tag_name = get_tag_dir_name(tag_id)
    await message.reply(f"✅ 频道 {chat_id} 已归类到「{tag_name}」")
```

**`/list_tags` — 列出所有频道标签**

```python
async def list_tags_command(client, message):
    """列出所有频道标签"""
    tags = get_all_tags()
    channel_tags = get_all_channel_tags()
    if not channel_tags:
        await message.reply("暂无频道标签设置")
        return
    lines = ["📋 频道标签列表:\n"]
    for chat_id, tag_id in sorted(channel_tags.items()):
        tag_name = get_tag_dir_name(tag_id)
        lines.append(f"  {chat_id} → {tag_name} ({tag_id})")
    await message.reply("\n".join(lines))
```

在 `_register_bot_handlers` 中注册：

```python
self.bot.add_handler(
    MessageHandler(
        set_tag_command,
        filters=pyrogram.filters.command(["set_tag"])
        & pyrogram.filters.user(self.allowed_user_ids),
    )
)
self.bot.add_handler(
    MessageHandler(
        list_tags_command,
        filters=pyrogram.filters.command(["list_tags"])
        & pyrogram.filters.user(self.allowed_user_ids),
    )
)
```

### 3.5 现有目录迁移

迁移脚本 `scripts/migrate_dirs.py`（手动运行，不在代码里自动执行）：

1. 扫描 `downloads/` 下所有一级目录
2. 根据目录名匹配分类规则（关键词匹配）
3. 创建 4 个大目录
4. `mv` 零散目录到大目录下
5. 生成 `channel_tags.yaml` 初稿（根据 chat_title → chat_id 映射）

关键词匹配规则：
```python
MIGRATION_RULES = {
    "jp_av": [],  # 日本AV目前都在 organized_strm 里，不在 downloads 下
    "western": ["onlyfans", "欧美", "US+EU", "捷克", "高清无码中字欧美", "搬运工", "异世界"],
    "domestic": ["国产", "糖心", "91", "射射屋", "麻豆", "自拍"],
    "special": ["福利姬", "Cosplay", "丝袜", "高跟", "巨乳", "美乳", "绅士仓库", "花园", "马里奥"],
}
```

### 3.6 Emby 集成

迁移完成后，在 Emby 中添加 4 个媒体库：
- 媒体库路径：`/volume1/docker/hermes-telegram-downloader/downloads/日本AV`
- 媒体库路径：`/volume1/docker/hermes-telegram-downloader/downloads/欧美`
- 媒体库路径：`/volume1/docker/hermes-telegram-downloader/downloads/国产`
- 媒体库路径：`/volume1/docker/hermes-telegram-downloader/downloads/特色`

内容类型设为"电影"或"混合"。

## 4. 文件改动清单

| 文件 | 改动 |
|------|------|
| `config.yaml` | 新增 `channel_tags` 和 `default_tag` 配置项 |
| `module/channel_tag.py` | **新建**：频道标签管理模块 |
| `module/app.py` | 修改 `get_file_save_path` 支持 chat_id 参数 |
| `media_downloader.py` | 调用 `get_file_save_path` 时传入 `chat_id` |
| `module/bot.py` | 新增 `/set_tag`、`/list_tags` 命令 + CallbackQueryHandler + 新频道弹窗逻辑 |
| `scripts/migrate_dirs.py` | **新建**：目录迁移脚本 |

## 5. 风险和注意事项

1. **路径变更影响已完成任务**：已下载的文件路径不变，只有新下载的文件走新路径
2. **Emby 刮削**：Emby 需要重新扫描新目录，可能需要调整刮削规则
3. **chat_title 变更**：TG 频道可能改名，但 chat_id 不变，标签绑定在 chat_id 上不受影响
4. **config 文件路径**：`channel_tags.yaml` 放在容器内 `/app/` 目录，需要挂载到宿主机持久化
5. **新频道弹窗的并发问题**：多个新频道同时下载时，弹窗需要关联到正确的 chat_id，用户选择后只触发对应 chat_id 的 pending consumer
6. **兼容性**：如果 `channel_tags.yaml` 不存在或为空，行为和现在一样（不加分目录），保证向后兼容

## 6. 实施步骤

1. **先实施方案代码改动**（channel_tag.py + app.py + bot.py + media_downloader.py）
2. **推到 NAS，重启容器**
3. **手动运行迁移脚本**归类现有目录
4. **在 Emby 中添加 4 个媒体库**
5. **用 `/set_tag` 给已知频道打标签**
6. **测试新频道下载弹窗**

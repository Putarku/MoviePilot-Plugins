import datetime
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.search import SearchChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType, NotificationType

lock = Lock()
_pending_lock = Lock()


class SubscribeStaleSearch(_PluginBase):
    """
    定时扫描超期未搜索的订阅，并逐个触发一次订阅搜索。
    """

    plugin_name = "订阅超期搜索"
    plugin_desc = "定时筛选超过指定天数未更新的订阅，并主动触发一次搜索后汇总通知。" \
                  "支持连续搜索无结果自动待定、待定订阅定期检查重新激活。"
    plugin_icon = "SubscribeStale.png"
    plugin_version = "1.2"
    plugin_author = "布丁"
    author_url = "https://github.com/Putarku"
    plugin_config_prefix = "subscribestalesearch_"
    plugin_order = 29
    auth_level = 1

    _scheduler: Optional[BackgroundScheduler] = None

    _enabled: bool = False
    _notify: bool = True
    _onlyonce: bool = False
    _cron: str = "0 3 * * *"
    _days: int = 7
    _states: List[str] = ["R", "P"]
    _media_types: List[str] = [MediaType.MOVIE.value, MediaType.TV.value]
    _stale_mode: str = "progress"
    _only_incomplete: bool = True
    _stat_enabled: bool = True
    _stat_search_mode: str = "id"
    _max_count: int = 0

    # 连续失败自动待定
    _auto_pending: bool = True
    _consecutive_limit: int = 3

    # 待定订阅定期检查
    _pending_check_enabled: bool = False
    _pending_check_cron: str = "0 4 * * 0"
    _pending_check_notify: bool = True

    def init_plugin(self, config: dict = None):
        """
        初始化插件配置，并处理立即执行任务。
        """
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._onlyonce = config.get("onlyonce", False)
            self._cron = config.get("cron", "0 3 * * *")
            self._days = self._safe_int(config.get("days"), 7)
            self._states = config.get("states") or ["R", "P"]
            self._media_types = config.get("media_types") or [MediaType.MOVIE.value, MediaType.TV.value]
            self._stale_mode = config.get("stale_mode") or "progress"
            self._only_incomplete = config.get("only_incomplete", True)
            self._stat_enabled = config.get("stat_enabled", True)
            self._stat_search_mode = config.get("stat_search_mode") or "id"
            self._max_count = max(self._safe_int(config.get("max_count"), 0), 0)
            # 连续失败自动待定
            self._auto_pending = config.get("auto_pending", True)
            self._consecutive_limit = max(self._safe_int(config.get("consecutive_limit"), 3), 1)
            # 待定订阅定期检查
            self._pending_check_enabled = config.get("pending_check_enabled", False)
            self._pending_check_cron = config.get("pending_check_cron") or "0 4 * * 0"
            self._pending_check_notify = config.get("pending_check_notify", True)

        # 清理失效的连续失败计数
        self._cleanup_consecutive_fails()

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.run,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                name="立即运行一次",
            )
            self._onlyonce = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def get_state(self) -> bool:
        """获取插件启用状态"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """获取插件命令定义"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """获取插件 API 定义"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件定时服务。
        """
        services = []
        if self._enabled and self._cron:
            services.append({
                "id": "SubscribeStaleSearch",
                "name": "订阅超期搜索",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run,
                "kwargs": {},
            })
        if self._enabled and self._pending_check_enabled and self._pending_check_cron:
            services.append({
                "id": "SubscribeStaleSearchPendingCheck",
                "name": "待定订阅检查",
                "trigger": CronTrigger.from_crontab(self._pending_check_cron),
                "func": self.run_pending_check,
                "kwargs": {},
            })
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        获取插件配置表单。
        """
        state_options = [
            {"title": "新建", "value": "N"},
            {"title": "订阅中", "value": "R"},
            {"title": "待定", "value": "P"},
            {"title": "暂停", "value": "S"},
        ]
        media_type_options = [
            {"title": "电影", "value": MediaType.MOVIE.value},
            {"title": "电视剧", "value": MediaType.TV.value},
        ]
        stale_mode_options = [
            {"title": "按进度更新时间", "value": "progress"},
            {"title": "按订阅创建时间", "value": "created"},
            {"title": "仅从未更新", "value": "never_updated"},
            {"title": "按创建或更新时间较新者", "value": "activity"},
        ]
        stat_mode_options = [
            {"title": "按ID精确统计", "value": "id"},
            {"title": "按标题关键词统计", "value": "title"},
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "发送汇总通知"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即运行一次"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "5位cron表达式",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "days",
                                            "label": "超期天数",
                                            "type": "number",
                                            "placeholder": "7",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_count",
                                            "label": "单次最多触发数",
                                            "type": "number",
                                            "placeholder": "0为不限制",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "states",
                                            "label": "筛选订阅状态",
                                            "items": state_options,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "media_types",
                                            "label": "筛选媒体类型",
                                            "items": media_type_options,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "stale_mode",
                                            "label": "超期判定方式",
                                            "items": stale_mode_options,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "only_incomplete", "label": "仅筛查未完成订阅"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "stat_enabled", "label": "启用搜索结果统计"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "stat_search_mode",
                                            "label": "搜索结果统计方式",
                                            "items": stat_mode_options,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "搜索结果统计会在正式触发订阅搜索前额外执行一次站点搜索，用于统计资源数和站点数。正式处理仍调用系统订阅搜索入口，插件不会改动核心逻辑。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "auto_pending",
                                            "label": "连续无结果自动待定",
                                            "hint": "启用后，订阅连续多次搜索均无进展时将自动置为待定状态",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "consecutive_limit",
                                            "label": "连续失败次数",
                                            "type": "number",
                                            "hint": "连续多少次搜索无进展后自动待定（默认3次）",
                                            "placeholder": "3",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "连续无结果自动待定：每次超期搜索时会记录订阅的搜索进展，连续多次搜索均无进展（无新增下载记录、缺失集数未减少）时将订阅自动置为待定状态，避免无效搜索。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "pending_check_enabled",
                                            "label": "启用待定订阅检查",
                                            "hint": "定期检查待定状态的订阅，重新搜索看是否有可用资源",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "pending_check_cron",
                                            "label": "检查周期",
                                            "placeholder": "5位cron表达式",
                                            "hint": "默认每周日凌晨4点执行",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "pending_check_notify",
                                            "label": "激活时发送通知",
                                            "hint": "有待定订阅被重新激活时发送通知",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "待定订阅检查：按设定周期对待定(P)状态的订阅重新触发搜索，如果搜索到可用资源则自动恢复为订阅中(R)状态，让已下线的资源在重新出现时能被自动抓取。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cron": "0 3 * * *",
            "days": 7,
            "states": ["R", "P"],
            "media_types": [MediaType.MOVIE.value, MediaType.TV.value],
            "stale_mode": "progress",
            "only_incomplete": True,
            "stat_enabled": True,
            "stat_search_mode": "id",
            "max_count": 0,
            "auto_pending": True,
            "consecutive_limit": 3,
            "pending_check_enabled": False,
            "pending_check_cron": "0 4 * * 0",
            "pending_check_notify": True,
        }

    def get_page(self) -> List[dict]:
        """
        获取插件详情页面（含执行记录、待定订阅、连续失败追踪）。
        """
        sections = []

        # -------- 1. 超期搜索执行记录 --------
        history = self.get_data("history") or []
        if history:
            rows = []
            for item in reversed(history[-20:]):
                rows.append({
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": item.get("time")},
                        {"component": "td", "text": item.get("summary")},
                        {"component": "td", "text": item.get("details") or item.get("titles")},
                    ],
                })
            sections.append({
                "component": "div",
                "props": {"class": "text-h6 mb-2 mt-4"},
                "text": "超期搜索执行记录",
            })
            sections.append({
                "component": "VTable",
                "props": {"hover": True, "density": "compact"},
                "content": [
                    {
                        "component": "thead",
                        "content": [{
                            "component": "tr",
                            "content": [
                                {"component": "th", "text": "执行时间"},
                                {"component": "th", "text": "执行摘要"},
                                {"component": "th", "text": "执行明细"},
                            ],
                        }],
                    },
                    {"component": "tbody", "content": rows},
                ],
            })
        else:
            sections.append({
                "component": "div",
                "props": {"class": "text-center mt-4"},
                "text": "暂无超期搜索执行记录",
            })

        # -------- 2. 待定订阅列表 --------
        try:
            pending_subs = SubscribeOper().list(state="P")
            if pending_subs:
                p_rows = []
                for sub in pending_subs:
                    title = self._format_subscribe_title(sub)
                    lack = sub.lack_episode or "-"
                    since = sub.last_update or sub.date or "-"
                    p_rows.append({
                        "component": "tr",
                        "content": [
                            {"component": "td", "text": title},
                            {"component": "td", "text": sub.type or "-"},
                            {"component": "td", "text": str(lack)},
                            {"component": "td", "text": since},
                        ],
                    })
                sections.append({
                    "component": "div",
                    "props": {"class": "text-h6 mb-2 mt-4"},
                    "text": f"待定订阅 (共 {len(pending_subs)} 个)",
                })
                sections.append({
                    "component": "VTable",
                    "props": {"hover": True, "density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [{
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "名称"},
                                    {"component": "th", "text": "类型"},
                                    {"component": "th", "text": "缺失"},
                                    {"component": "th", "text": "最后更新"},
                                ],
                            }],
                        },
                        {"component": "tbody", "content": p_rows},
                    ],
                })
        except Exception as err:
            logger.error(f"获取待定订阅列表失败：{err}")

        # -------- 3. 连续失败追踪 --------
        consec_data = self.get_data("consecutive_fails") or {}
        if consec_data:
            c_rows = []
            # 获取订阅名称
            for sid_str, count in sorted(consec_data.items(), key=lambda x: x[1], reverse=True):
                try:
                    sub = SubscribeOper().get(int(sid_str))
                    if not sub:
                        continue
                    title = self._format_subscribe_title(sub)
                except (ValueError, TypeError):
                    title = f"ID:{sid_str}"
                limit = self._consecutive_limit
                bar = "■" * min(count, limit) + "□" * max(limit - count, 0)
                c_rows.append({
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": title},
                        {"component": "td", "text": f"{count}/{limit}"},
                        {"component": "td", "text": bar},
                    ],
                })
            if c_rows:
                sections.append({
                    "component": "div",
                    "props": {"class": "text-h6 mb-2 mt-4"},
                    "text": "连续失败追踪 (即将自动待定的订阅)",
                })
                sections.append({
                    "component": "VTable",
                    "props": {"hover": True, "density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [{
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "名称"},
                                    {"component": "th", "text": "连续失败"},
                                    {"component": "th", "text": "进度"},
                                ],
                            }],
                        },
                        {"component": "tbody", "content": c_rows},
                    ],
                })

        # -------- 4. 待定订阅检查记录 --------
        pend_history = self.get_data("pending_check_history") or []
        if pend_history:
            ph_rows = []
            for item in reversed(pend_history[-20:]):
                detail_parts = item.get("detail_parts", {})
                reactivated = detail_parts.get("reactivated", [])
                detail_text = (
                    f"已激活 {len(reactivated)} 个"
                    if reactivated else "无变化"
                )
                ph_rows.append({
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": item.get("time")},
                        {"component": "td", "text": item.get("summary")},
                        {"component": "td", "text": detail_text},
                    ],
                })
            sections.append({
                "component": "div",
                "props": {"class": "text-h6 mb-2 mt-4"},
                "text": "待定订阅检查记录",
            })
            sections.append({
                "component": "VTable",
                "props": {"hover": True, "density": "compact"},
                "content": [
                    {
                        "component": "thead",
                        "content": [{
                            "component": "tr",
                            "content": [
                                {"component": "th", "text": "执行时间"},
                                {"component": "th", "text": "执行摘要"},
                                {"component": "th", "text": "激活详情"},
                            ],
                        }],
                    },
                    {"component": "tbody", "content": ph_rows},
                ],
            })

        return sections if sections else [
            {
                "component": "div",
                "text": "暂无数据",
                "props": {"class": "text-center mt-4"},
            }
        ]

    def stop_service(self):
        """
        停止插件服务。
        """
        if self._scheduler:
            try:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
            except Exception as err:
                logger.error(f"停止订阅超期搜索服务失败：{err}")
            finally:
                self._scheduler = None

    def _cleanup_consecutive_fails(self):
        """
        清除已失效的连续失败计数（订阅已删除、已完成等）。
        """
        consec_data = self.get_data("consecutive_fails") or {}
        if not consec_data:
            return

        stale_ids = []
        for sid_str in consec_data:
            try:
                sub = SubscribeOper().get(int(sid_str))
                if sub is None:
                    # 订阅已被删除
                    stale_ids.append(sid_str)
                elif sub.state not in ["N", "R", "P"]:
                    # 已完成或暂停的订阅不再需要追踪
                    stale_ids.append(sid_str)
            except (ValueError, TypeError):
                stale_ids.append(sid_str)

        if stale_ids:
            for sid in stale_ids:
                consec_data.pop(sid, None)
            self.save_data("consecutive_fails", consec_data)
            logger.info(f"清理了 {len(stale_ids)} 个失效的连续失败计数")

    def run(self):
        """
        执行超期订阅扫描与搜索。
        """
        if not self._enabled and not self._scheduler:
            return

        if not lock.acquire(blocking=False):
            logger.warning("订阅超期搜索任务正在执行中，跳过本次运行")
            return

        # 每次运行前清理失效的连续失败计数
        self._cleanup_consecutive_fails()

        try:
            now = datetime.datetime.now()
            subscribes = self._list_target_subscribes(now=now)
            total = len(subscribes)
            success_count = 0
            failed_items: List[str] = []
            titles: List[str] = []
            result_stats: List[Dict[str, Any]] = []

            logger.info(f"订阅超期搜索开始，共命中 {total} 个订阅")

            for subscribe in subscribes:
                try:
                    title = self._format_subscribe_title(subscribe)
                    titles.append(title)
                    logger.info(f"开始触发订阅搜索：{title}")
                    before_snapshot = self._build_subscribe_snapshot(subscribe)
                    search_stat = self._preview_search_safe(subscribe) if self._stat_enabled else {}
                    SubscribeChain().search(sid=subscribe.id, manual=False)
                    after_subscribe = SubscribeOper().get(subscribe.id)
                    result_stat = self._build_result_stat(
                        subscribe=after_subscribe or subscribe,
                        before_snapshot=before_snapshot,
                        search_stat=search_stat,
                    )
                    result_stats.append(result_stat)
                    success_count += 1

                    # 连续失败自动待定追踪
                    if self._auto_pending and self._consecutive_limit > 0:
                        self._track_consecutive(subscribe, result_stat)

                except Exception as err:
                    title = self._format_subscribe_title(subscribe)
                    logger.error(f"触发订阅搜索失败：{title}，原因：{err}")
                    failed_items.append(title)
                    result_stats.append({
                        "title": title,
                        "status": "失败",
                        "message": str(err),
                        "resources": 0,
                        "sites": 0,
                    })
                    # 异常也计为无进展
                    if self._auto_pending and self._consecutive_limit > 0:
                        self._track_consecutive(subscribe, {
                            "title": title,
                            "status": "失败",
                            "message": str(err),
                            "resources": 0,
                            "sites": 0,
                        })

            summary = f"命中 {total} 个订阅，成功触发 {success_count} 个，失败 {len(failed_items)} 个"
            logger.info(f"订阅超期搜索完成，{summary}")
            self._save_history(summary=summary, titles=titles, result_stats=result_stats)
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="订阅超期搜索完成",
                    text=self._build_notify_text(summary=summary, titles=titles, failed_items=failed_items,
                                                result_stats=result_stats),
                )
        finally:
            lock.release()

    def _track_consecutive(self, subscribe: Any, result_stat: Dict[str, Any]):
        """
        跟踪订阅的连续无进展次数，达到阈值时自动置为待定。
        """
        progressed = result_stat.get("status") == "有进展"
        sid = str(subscribe.id)

        consec_data = self.get_data("consecutive_fails") or {}

        if progressed:
            # 搜索有进展，重置计数
            if sid in consec_data:
                logger.info(
                    f"订阅 {self._format_subscribe_title(subscribe)} 搜索有进展，"
                    f"重置连续无进展计数"
                )
                consec_data.pop(sid)
        else:
            # 搜索无进展（无结果或失败），增加计数
            count = consec_data.get(sid, 0) + 1
            consec_data[sid] = count
            logger.info(
                f"订阅 {self._format_subscribe_title(subscribe)} "
                f"连续 {count} 次搜索未找到可用资源"
            )

            if count >= self._consecutive_limit and subscribe.state == "R":
                # 将订阅置为待定状态
                SubscribeOper().update(subscribe.id, {"state": "P"})
                logger.info(
                    f"订阅 {self._format_subscribe_title(subscribe)} "
                    f"已达连续 {count} 次无可用资源，已自动置为待定状态"
                )
                # 置为待定后移除计数
                consec_data.pop(sid, None)

                if self._notify:
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title="订阅已自动暂停",
                        text=(
                            f"订阅 {self._format_subscribe_title(subscribe)} "
                            f"已连续 {count} 次搜索未找到可用资源，"
                            f"已自动设置为待定(P)状态，将停止对该订阅的定期搜索。"
                        ),
                    )

        self.save_data("consecutive_fails", consec_data)

    def run_pending_check(self):
        """
        检查待定(P)状态的订阅，重新触发搜索以捕捉重新出现的资源。
        如果搜索触发下载，订阅状态会自动变回订阅中(R)。
        """
        if not self._enabled:
            return

        if not _pending_lock.acquire(blocking=False):
            logger.warning("待定订阅检查任务正在执行中，跳过本次运行")
            return

        try:
            pending_subs = SubscribeOper().list(state="P")
            if not pending_subs:
                logger.info("待定订阅检查：暂无待定状态的订阅")
                return

            logger.info(f"待定订阅检查开始，共 {len(pending_subs)} 个待定订阅")

            reactivated: List[str] = []
            still_pending: List[str] = []
            failed: List[str] = []

            for subscribe in pending_subs:
                try:
                    title = self._format_subscribe_title(subscribe)
                    logger.info(f"重新搜索待定订阅：{title}")
                    before_state = subscribe.state
                    SubscribeChain().search(sid=subscribe.id, manual=False)
                    after_subscribe = SubscribeOper().get(subscribe.id)

                    if after_subscribe and after_subscribe.state != before_state:
                        reactivated.append(title)
                        logger.info(
                            f"待定订阅 {title} 已搜索到资源，"
                            f"状态从 {before_state} 变更为 {after_subscribe.state}"
                        )
                    else:
                        still_pending.append(title)
                except Exception as err:
                    title = self._format_subscribe_title(subscribe)
                    logger.error(f"待定订阅 {title} 重新搜索失败：{err}")
                    failed.append(title)

            summary = (
                f"检查待定订阅 {len(pending_subs)} 个，"
                f"已激活 {len(reactivated)} 个，"
                f"仍待定 {len(still_pending)} 个"
            )
            if failed:
                summary += f"，失败 {len(failed)} 个"
            logger.info(f"待定订阅检查完成，{summary}")

            # 保存执行记录
            history = self.get_data("pending_check_history") or []
            history.append({
                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "summary": summary,
                "detail_parts": {
                    "reactivated": reactivated[:10],
                    "still_pending": still_pending[:10],
                    "failed": failed[:10],
                },
            })
            self.save_data("pending_check_history", history[-30:])

            # 发送通知
            if self._pending_check_notify and reactivated:
                notify_text = summary
                if reactivated:
                    notify_text += "\n\n已重新激活：\n" + "\n".join(
                        f"- {t}" for t in reactivated[:10]
                    )
                    if len(reactivated) > 10:
                        notify_text += f"\n...及其他 {len(reactivated) - 10} 个"
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="待定订阅检查完成",
                    text=notify_text,
                )
        finally:
            _pending_lock.release()

    def _list_target_subscribes(self, now: datetime.datetime) -> List[Any]:
        """
        获取所有符合超期条件的订阅。
        """
        state = ",".join(self._states) if self._states else None
        subscribes = SubscribeOper().list(state)
        target_subscribes = []
        for subscribe in subscribes:
            if self._media_types and subscribe.type not in self._media_types:
                continue
            if self._only_incomplete and not self._is_incomplete(subscribe):
                continue
            reference_time = self._get_stale_reference_time(subscribe)
            if not reference_time:
                continue
            if (now - reference_time).days < max(self._days, 0):
                continue
            target_subscribes.append(subscribe)
        target_subscribes.sort(
            key=lambda item: self._get_stale_reference_time(item) or now
        )
        if self._max_count:
            return target_subscribes[:self._max_count]
        return target_subscribes

    def _build_notify_text(self, summary: str, titles: List[str], failed_items: List[str],
                           result_stats: List[Dict[str, Any]]) -> str:
        """
        构建汇总通知内容（分组归类、去除零值噪音）。
        """
        config_line = (
            f"筛选: {','.join(self._states) if self._states else '全部'} | "
            f"{'、'.join(self._media_types) if self._media_types else '全部'} | "
            f"超期 {self._days}天({self._stale_mode_label()})"
        )

        parts = ["═══ 订阅超期搜索报告 ═══", "", config_line, summary, ""]

        # 按是否有进展和有资源分组
        progressed = [s for s in result_stats if s.get("status") == "有进展"]
        no_progress_with_res = [
            s for s in result_stats
            if s.get("status") != "有进展" and s.get("resources", 0) > 0
        ]
        no_progress_no_res = [
            s for s in result_stats
            if s.get("status") != "有进展" and s.get("resources", 0) == 0
        ]

        if progressed:
            parts.append(f"✓ 有进展 ({len(progressed)})")
            for item in progressed[:20]:
                parts.append(f"  {self._format_result_stat(item)}")
            parts.append("")

        if no_progress_with_res:
            parts.append(f"~ 有资源但无进展 ({len(no_progress_with_res)})")
            for item in no_progress_with_res[:20]:
                parts.append(f"  {self._format_result_stat(item)}")
            parts.append("")

        if no_progress_no_res:
            parts.append(f"× 无可用资源 ({len(no_progress_no_res)})")
            for item in no_progress_no_res[:20]:
                parts.append(f"  {self._format_result_stat(item)}")
            parts.append("")

        if len(result_stats) > 20:
            parts.append(f"...及其他 {len(result_stats) - 20} 个订阅")

        if failed_items:
            parts.append(f"! 执行失败 ({len(failed_items)})")
            for title in failed_items[:20]:
                parts.append(f"  {title}")
            parts.append("")

        return "\n".join(parts).rstrip()

    def _save_history(self, summary: str, titles: List[str], result_stats: List[Dict[str, Any]]):
        """
        保存执行历史。
        """
        history = self.get_data("history") or []
        history.append({
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": summary,
            "titles": "、".join(titles[:10]) if titles else "无",
            "details": "；".join(self._format_result_stat(item) for item in result_stats[:10]) if result_stats else "无",
        })
        self.save_data("history", history[-50:])

    def _preview_search(self, subscribe: Any) -> Dict[str, Any]:
        """
        预搜索订阅资源，用于在不修改核心代码的前提下生成结果级统计。
        """
        if self._stat_search_mode == "title":
            contexts = SearchChain().search_by_title(
                title=subscribe.keyword or subscribe.name,
                sites=SubscribeChain.get_sub_sites(subscribe),
            )
        else:
            contexts = SearchChain().search_by_id(
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                mtype=MediaType(subscribe.type),
                area="imdbid" if subscribe.search_imdbid else "title",
                season=subscribe.season,
                sites=SubscribeChain.get_sub_sites(subscribe),
            )
        contexts = contexts or []
        sites = {context.torrent_info.site_name or context.torrent_info.site for context in contexts}
        return {
            "resources": len(contexts),
            "sites": len([site for site in sites if site]),
        }

    def _preview_search_safe(self, subscribe: Any) -> Dict[str, Any]:
        """
        安全执行预搜索统计，统计失败不影响正式订阅搜索。
        """
        try:
            return self._preview_search(subscribe)
        except Exception as err:
            logger.error(f"订阅 {self._format_subscribe_title(subscribe)} 预搜索统计失败：{err}")
            return {"resources": 0, "sites": 0, "stat_error": str(err)}

    def _build_subscribe_snapshot(self, subscribe: Any) -> Dict[str, Any]:
        """
        记录订阅搜索前的关键字段，用于搜索后对比进展。
        """
        return {
            "lack_episode": subscribe.lack_episode,
            "note_count": len(subscribe.note or []),
            "last_update": subscribe.last_update,
            "state": subscribe.state,
        }

    def _build_result_stat(self, subscribe: Any, before_snapshot: Dict[str, Any],
                           search_stat: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据预搜索结果和订阅字段变化生成单个订阅统计。
        """
        after_snapshot = self._build_subscribe_snapshot(subscribe)
        lack_before = before_snapshot.get("lack_episode")
        lack_after = after_snapshot.get("lack_episode")
        note_delta = after_snapshot.get("note_count", 0) - before_snapshot.get("note_count", 0)
        lack_delta = 0
        if lack_before is not None and lack_after is not None:
            lack_delta = max(int(lack_before) - int(lack_after), 0)
        progressed = note_delta > 0 or lack_delta > 0 or before_snapshot.get("state") != after_snapshot.get("state")
        return {
            "title": self._format_subscribe_title(subscribe),
            "status": "有进展" if progressed else "无进展",
            "resources": search_stat.get("resources", 0),
            "sites": search_stat.get("sites", 0),
            "note_delta": max(note_delta, 0),
            "lack_delta": lack_delta,
            "lack_after": lack_after,
        }

    @staticmethod
    def _format_result_stat(item: Dict[str, Any]) -> str:
        """
        格式化单个订阅的统计信息（简洁版，隐藏零值字段）。
        """
        if item.get("message"):
            return f"{item.get('title')}: {item.get('message')}"

        title = item.get('title', '')
        status = item.get('status', '')
        resources = item.get('resources', 0)
        sites = item.get('sites', 0)
        note_delta = item.get('note_delta', 0)
        lack_delta = item.get('lack_delta', 0)
        lack_after = item.get('lack_after')

        if status == "有进展":
            parts = [f"{title}: ✓"]
            if note_delta > 0:
                parts.append(f"新增记录+{note_delta}")
            if lack_delta > 0:
                parts.append(f"缺失减少{lack_delta}")
            if lack_after is not None:
                parts.append(f"剩余缺失{lack_after}")
            return " | ".join(parts)

        # 无进展
        parts = [title]
        if resources > 0:
            res = f"资源{resources}"
            if sites > 0:
                res += f"({sites}站)"
            parts.append(res)
        else:
            parts.append("无资源")
        if lack_after is not None:
            parts.append(f"缺失{lack_after}集" if lack_after else "已完成")
        return " | ".join(parts)

    def _get_stale_reference_time(self, subscribe: Any) -> Optional[datetime.datetime]:
        """
        根据配置返回订阅超期判定的参考时间。
        """
        created_time = self._parse_datetime(subscribe.date)
        update_time = self._parse_datetime(subscribe.last_update)
        if self._stale_mode == "created":
            return created_time
        if self._stale_mode == "never_updated":
            return created_time if not update_time else None
        if self._stale_mode == "activity":
            return max([item for item in [created_time, update_time] if item], default=None)
        return update_time or created_time

    @staticmethod
    def _is_incomplete(subscribe: Any) -> bool:
        """
        判断订阅是否仍存在未完成内容。
        """
        if subscribe.state in ["S"]:
            return False
        if subscribe.type == MediaType.MOVIE.value:
            return not subscribe.note
        if subscribe.lack_episode is None:
            return True
        try:
            return int(subscribe.lack_episode or 0) > 0
        except (TypeError, ValueError):
            return True

    def _stale_mode_label(self) -> str:
        """
        返回超期判定方式的展示名称。
        """
        return {
            "progress": "按进度更新时间",
            "created": "按订阅创建时间",
            "never_updated": "仅从未更新",
            "activity": "按创建或更新时间较新者",
        }.get(self._stale_mode, self._stale_mode)

    def _format_subscribe_title(self, subscribe: Any) -> str:
        """
        格式化订阅展示标题。
        """
        if subscribe.season is not None:
            return f"{subscribe.name} S{int(subscribe.season):02d}"
        return subscribe.name

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime.datetime]:
        """
        解析订阅时间字段。
        """
        if not value:
            return None
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        """
        安全转换整数配置值。
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def __update_config(self):
        """
        保存插件配置。
        """
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "days": self._days,
            "states": self._states,
            "media_types": self._media_types,
            "stale_mode": self._stale_mode,
            "only_incomplete": self._only_incomplete,
            "stat_enabled": self._stat_enabled,
            "stat_search_mode": self._stat_search_mode,
            "max_count": self._max_count,
            "auto_pending": self._auto_pending,
            "consecutive_limit": self._consecutive_limit,
            "pending_check_enabled": self._pending_check_enabled,
            "pending_check_cron": self._pending_check_cron,
            "pending_check_notify": self._pending_check_notify,
        })

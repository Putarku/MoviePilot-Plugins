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


class SubscribeStaleSearch(_PluginBase):
    """
    定时扫描超期未搜索的订阅，并逐个触发一次订阅搜索。
    """

    plugin_name = "订阅超期搜索"
    plugin_desc = "定时筛选超过指定天数未更新的订阅，并主动触发一次搜索后汇总通知。"
    plugin_icon = "SubscribeStale.png"
    plugin_version = "1.1"
    plugin_author = "布丁"
    author_url = "https://github.com"
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
        if not self._enabled or not self._cron:
            return []
        return [{
            "id": "SubscribeStaleSearch",
            "name": "订阅超期搜索",
            "trigger": CronTrigger.from_crontab(self._cron),
            "func": self.run,
            "kwargs": {},
        }]

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
        }

    def get_page(self) -> List[dict]:
        """
        获取插件详情页面。
        """
        history = self.get_data("history") or []
        if not history:
            return [
                {
                    "component": "div",
                    "text": "暂无执行记录",
                    "props": {"class": "text-center mt-4"},
                }
            ]

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

        return [
            {
                "component": "VTable",
                "props": {"hover": True},
                "content": [
                    {
                        "component": "thead",
                        "content": [
                            {
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "执行时间"},
                                    {"component": "th", "text": "执行摘要"},
                                    {"component": "th", "text": "执行明细"},
                                ],
                            }
                        ],
                    },
                    {"component": "tbody", "content": rows},
                ],
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

    def run(self):
        """
        执行超期订阅扫描与搜索。
        """
        if not self._enabled and not self._scheduler:
            return

        if not lock.acquire(blocking=False):
            logger.warning("订阅超期搜索任务正在执行中，跳过本次运行")
            return

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
                    result_stats.append(self._build_result_stat(
                        subscribe=after_subscribe or subscribe,
                        before_snapshot=before_snapshot,
                        search_stat=search_stat,
                    ))
                    success_count += 1
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
        构建汇总通知内容。
        """
        lines = [
            f"筛选状态：{','.join(self._states) if self._states else '全部'}",
            f"媒体类型：{'、'.join(self._media_types) if self._media_types else '全部'}",
            f"超期判定：{self._stale_mode_label()}",
            f"超期天数：{self._days} 天",
            summary,
        ]
        if result_stats:
            preview = "\n".join(self._format_result_stat(item) for item in result_stats[:20])
            lines.append(f"执行明细：\n{preview}")
            if len(result_stats) > 20:
                lines.append(f"其余 {len(result_stats) - 20} 个订阅未展开显示")
        else:
            lines.append("本次没有命中需要补搜的订阅")

        if failed_items:
            failed_preview = "\n".join(f"- {title}" for title in failed_items[:20])
            lines.append(f"触发失败：\n{failed_preview}")
        return "\n".join(lines)

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
        格式化单个订阅的统计信息。
        """
        if item.get("message"):
            return f"- {item.get('title')}：{item.get('status')}，{item.get('message')}"
        return (
            f"- {item.get('title')}：{item.get('status')}，"
            f"资源 {item.get('resources', 0)}，站点 {item.get('sites', 0)}，"
            f"新增记录 {item.get('note_delta', 0)}，缺失减少 {item.get('lack_delta', 0)}，"
            f"剩余缺失 {item.get('lack_after') if item.get('lack_after') is not None else '-'}"
        )

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
        })

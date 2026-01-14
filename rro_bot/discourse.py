from __future__ import annotations

from dataclasses import dataclass

import aiohttp


@dataclass(frozen=True)
class DiscourseTopic:
    id: int
    title: str
    slug: str
    url: str
    category_id: int
    tags: list[str]
    author: str


class DiscourseClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        api_user: str,
        session: aiohttp.ClientSession,
    ):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._api_user = api_user
        self._session = session

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._api_key:
            headers["Api-Key"] = self._api_key
        if self._api_user:
            headers["Api-Username"] = self._api_user
        return headers

    async def fetch_topic(self, topic_id: int) -> DiscourseTopic:
        url = f"{self._base_url}/t/{topic_id}.json"
        async with self._session.get(
            url,
            headers=self._headers(),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            r.raise_for_status()
            data = await r.json()

        topic = data.get("topic") or data
        tags = list(topic.get("tags") or [])
        slug = topic.get("slug") or str(topic_id)
        title = topic.get("title") or f"Topic {topic_id}"
        category_id = int(topic.get("category_id") or 0)

        author = "Unknown"
        # Discourse topic JSON varies; try several common locations.
        created_by = topic.get("created_by")
        if isinstance(created_by, dict):
            author = created_by.get("username") or created_by.get("name") or author

        if author == "Unknown":
            details = topic.get("details")
            if isinstance(details, dict):
                details_created_by = details.get("created_by")
                if isinstance(details_created_by, dict):
                    author = (
                        details_created_by.get("username")
                        or details_created_by.get("name")
                        or author
                    )

        if author == "Unknown":
            # Fallback to the first post in the stream.
            post_stream = data.get("post_stream")
            if isinstance(post_stream, dict):
                posts = post_stream.get("posts")
                if isinstance(posts, list) and posts:
                    first = posts[0]
                    if isinstance(first, dict):
                        author = first.get("username") or first.get("name") or author

        return DiscourseTopic(
            id=int(topic.get("id") or topic_id),
            title=title,
            slug=slug,
            url=f"{self._base_url}/t/{slug}/{topic_id}",
            category_id=category_id,
            tags=tags,
            author=author,
        )

    async def set_topic_tags(self, topic_id: int, tags: list[str]) -> None:
        if not self._api_key or not self._api_user:
            raise RuntimeError(
                "Discourse API credentials missing (DISCOURSE_API_KEY / DISCOURSE_API_USER)"
            )

        url = f"{self._base_url}/t/{topic_id}.json"
        if tags:
            form: list[tuple[str, str]] = [("tags[]", t) for t in tags]
        else:
            # Discourse expects an explicit empty tag array to clear tags.
            form = [("tags[]", "")]

        async with self._session.put(
            url,
            headers=self._headers(),
            data=form,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            r.raise_for_status()

    async def set_topic_title(self, topic_id: int, title: str) -> None:
        if not self._api_key or not self._api_user:
            raise RuntimeError(
                "Discourse API credentials missing (DISCOURSE_API_KEY / DISCOURSE_API_USER)"
            )

        url = f"{self._base_url}/t/{topic_id}.json"
        form: list[tuple[str, str]] = [("title", title)]
        async with self._session.put(
            url,
            headers=self._headers(),
            data=form,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            r.raise_for_status()

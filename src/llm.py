"""The project's only LLM call site: structured generation with retries.

Same pattern as the weather projects: the response schema guarantees the
*shape* of what comes back, not whether it's *true* - callers validate the
content (see briefing.validate).
"""

from functools import lru_cache

from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from config import GEMINI_API_KEY


@lru_cache(maxsize=1)
def _client() -> genai.Client:
    # Without a timeout a stalled request hangs the whole daily job (it did,
    # for 15+ minutes, on the first try). 2 minutes, then retry or give up.
    return genai.Client(api_key=GEMINI_API_KEY, http_options=types.HttpOptions(timeout=120_000))


def _is_retryable(exception: BaseException) -> bool:
    # 503 overload and 429 rate limit fix themselves with time; 400/403 don't.
    if isinstance(exception, genai_errors.ServerError):
        return True
    return isinstance(exception, genai_errors.ClientError) and exception.code == 429


@retry(retry=retry_if_exception(_is_retryable), stop=stop_after_attempt(2),
       wait=wait_exponential(multiplier=2, min=5, max=60), reraise=True)
def generate(prompt: str, schema, model: str):
    """Returns an instance of `schema` (a Pydantic model), or None if the model returned nothing parseable."""
    response = _client().models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema,
            temperature=0.2,
        ),
    )
    return response.parsed

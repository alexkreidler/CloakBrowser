"""Example: capturing network traffic as HAR via CloakBrowser.

CloakBrowser exposes Playwright's HAR recording as first-class options on
``launch_context()`` and ``launch_persistent_context()``.  The HAR is written
when the context is closed.

For full CDP capture (every command/event, not just network), see
``cloakserve --record-dir=...`` instead — that path records traffic from
*any* CDP client (Playwright, Puppeteer, ``agent-browser``, ...).
"""

from cloakbrowser import launch_context


def main() -> None:
    ctx = launch_context(
        headless=True,
        har_path="example.har",
        har_mode="full",
        har_content="embed",
        # har_url_filter="**/api/**",  # optional: restrict to API calls
    )
    page = ctx.new_page()
    page.goto("https://example.com")
    page.goto("https://example.org")
    ctx.close()  # HAR is written here.
    print("Wrote example.har")


if __name__ == "__main__":
    main()

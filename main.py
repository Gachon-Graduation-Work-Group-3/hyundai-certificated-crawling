import asyncio
import crawling
import extract_links


if __name__ == "__main__":
    extract_links()
    asyncio.run(crawling.main())
    
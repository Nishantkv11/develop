import csv
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote_plus, urlparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SearchKeywordRevenueProcessor:
    """
    Processes hit-level TSV data and creates the final keyword revenue report.

    Business logic used:
    - Detect external search engine visits from referrer URL
    - Extract search keyword from query parameters
    - Detect purchase rows using event_list
    - Extract revenue from product_list
    - Attribute purchase revenue to the most recent external search
      keyword seen for the same IP

    Why IP-based attribution?
    - In your sample file, purchase rows use internal checkout referrers,
      while earlier rows for the same IP contain the search engine referrer.
    - So this simplified solution keeps the most recent search touchpoint
      by IP and attributes later purchase revenue to it.
    """

    SEARCH_ENGINES = {
        "google": {"domains": ["google."], "params": ["q"]},
        "yahoo": {"domains": ["search.yahoo.", "yahoo."], "params": ["p"]},
        "bing": {"domains": ["bing.com", "msn.com", "search.msn."], "params": ["q"]}
    }

    def __init__(self, input_file: str):
        self.input_file = input_file

        # Stores latest (search_engine_domain, keyword) for each IP
        self.attribution_state: Dict[str, Tuple[str, str]] = {}

        # Stores final aggregated revenue:
        # key = (domain, keyword), value = total revenue
        self.results: Dict[Tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0.00"))

        # Execution counters
        self.records_processed = 0
        self.purchase_rows = 0
        self.attributed_purchases = 0
        self.unattributed_purchases = 0

    def detect_search_engine_and_keyword(self, referrer: str) -> Optional[Tuple[str, str]]:
        """
        Detect whether the referrer belongs to a supported search engine
        and extract its keyword.

        Returns:
            (domain, keyword) if found
            None otherwise
        """
        if not referrer:
            return None

        try:
            parsed = urlparse(referrer.strip())
            domain = (parsed.netloc or "").lower()
            query = parse_qs(parsed.query)
        except Exception:
            return None

        for _, config in self.SEARCH_ENGINES.items():
            if any(token in domain for token in config["domains"]):
                for param in config["params"]:
                    values = query.get(param)
                    if values and values[0].strip():
                        keyword = unquote_plus(values[0].strip()).lower()
                        return domain, keyword

        return None

    def is_purchase_event(self, event_list: str) -> bool:
        """
        Check whether this row represents a purchase event.

        Based on your PDF/appended sample logic:
        event 1 indicates purchase.
        """
        if not event_list:
            return False

        events = [e.strip() for e in event_list.split(",") if e.strip()]
        return "1" in events

    def extract_revenue(self, product_list: str) -> Decimal:
        """
        Extract total revenue from product_list.

        Expected simplified format per product:
        Category;Product Name;Number of Items;Total Revenue;...

        Multiple products are comma-separated.
        """
        if not product_list:
            return Decimal("0.00")

        total = Decimal("0.00")
        products = [p.strip() for p in product_list.split(",") if p.strip()]

        for product in products:
            parts = product.split(";")
            if len(parts) >= 4:
                revenue_str = parts[3].strip()
                if revenue_str:
                    try:
                        total += Decimal(revenue_str)
                    except InvalidOperation:
                        logger.warning("Invalid revenue value found: %s", revenue_str)

        return total

    def process(self) -> List[Tuple[str, str, Decimal]]:
        """
        Main business processing flow.

        Returns:
            Sorted list of (search_engine_domain, keyword, revenue)
            ordered by revenue descending.
        """
        with open(self.input_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")

            # Business processing requires these columns
            required_columns = {"ip", "referrer", "event_list", "product_list"}
            missing = required_columns - set(reader.fieldnames or [])
            if missing:
                raise ValueError(
                    f"Missing required columns for business processing: {sorted(missing)}"
                )

            for row in reader:
                self.records_processed += 1

                ip = (row.get("ip") or "").strip()
                referrer = (row.get("referrer") or "").strip()
                event_list = (row.get("event_list") or "").strip()
                product_list = (row.get("product_list") or "").strip()

                # Track the most recent external search touchpoint for this IP
                search_info = self.detect_search_engine_and_keyword(referrer)
                if ip and search_info:
                    self.attribution_state[ip] = search_info

                # If row is a purchase, attribute revenue to most recent search touchpoint
                if self.is_purchase_event(event_list):
                    self.purchase_rows += 1
                    revenue = self.extract_revenue(product_list)

                    if revenue > 0 and ip in self.attribution_state:
                        domain, keyword = self.attribution_state[ip]
                        self.results[(domain, keyword)] += revenue
                        self.attributed_purchases += 1
                    else:
                        self.unattributed_purchases += 1

        sorted_rows = sorted(
            [(domain, keyword, revenue) for (domain, keyword), revenue in self.results.items()],
            key=lambda x: x[2],
            reverse=True
        )
        return sorted_rows

    def write_output(self, output_dir: str = ".") -> str:
        """
        Write final tab-delimited report to local disk.

        File name format:
            YYYY-mm-dd_SearchKeywordPerformance.tab
        """
        rows = self.process()
        file_name = f"{datetime.utcnow().strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"
        output_path = os.path.join(output_dir, file_name)

        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["Search Engine Domain", "Search Keyword", "Revenue"])

            for domain, keyword, revenue in rows:
                writer.writerow([domain, keyword, f"{revenue:.2f}"])

        logger.info("Output written to %s", output_path)
        return output_path

    def summary(self) -> dict:
        """
        Return summary metrics for logging/audit.
        """
        return {
            "input_file": self.input_file,
            "records_processed": self.records_processed,
            "purchase_rows": self.purchase_rows,
            "attributed_purchases": self.attributed_purchases,
            "unattributed_purchases": self.unattributed_purchases,
            "status": "SUCCESS",
            "processed_at_utc": datetime.utcnow().isoformat()
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process search keyword revenue file.")
    parser.add_argument("input_file", help="Path to input TSV file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    processor = SearchKeywordRevenueProcessor(args.input_file)
    output_file = processor.write_output(".")
    print(f"Created: {output_file}")
    print(json.dumps(processor.summary(), indent=2))
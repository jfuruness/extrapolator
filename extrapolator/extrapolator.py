from datetime import datetime

from mrt_collector import MRTCollector

class Extrapolator:
    def run(self):
        """See steps below

        1. Run the MRT Collector
            a. This downloads ROV info, hijack info, etc
            b. This downloads all MRT RIB dumps and formats them for extrapolation
        2.
        """

        collector = MRTCollector(dl_time=datetime(2023, 12, 12, 0, 0, 0), cpus=1)
        collector.run(max_block_size=1000)
        # TODO: total number of anns, prefixes, and as rank
        raise NotImplementedError("add to mrt collector to get vantage points stats")
        raise NotImplementedError("run for quick amnt")
        raise NotImplementedError("run for full amnt and go on a walk")
        raise NotImplementedError("select top 10 vantage points, removing ribs in")
        raise NotImplementedError("add vantage point data to appropriate ppt slide")
        raise NotImplementedError(
            "select prefixes that exist at all vantage points, omit path poisoning"
        )
        raise NotImplementedError("for each vantage point, omit, propagate, levenshtein")
        raise NotImplementedError("statistical significance")
        raise NotImplementedError("run for normal")
        raise NotImplementedError("add to ppt")
        raise NotImplementedError("run for seeding along as path")
        raise NotImplementedError("run for rov + seeding along the as path")
        raise NotImplementedError("if ROV doesn't matter, remove stubs?")
        raise NotImplementedError("run for multihomed provider preference")
        raise NotImplementedError("add to ppt, go through docs")
        raise NotImplementedError("prob link?")

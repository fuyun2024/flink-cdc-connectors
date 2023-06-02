package com.ververica.cdc.connectors.base.source.reader.external.rate;

import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;

public abstract class RateLimitFetcher implements Fetcher<SourceRecords, SourceSplitBase> {}

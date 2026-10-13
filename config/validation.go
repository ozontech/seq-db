package config

import (
	"cmp"
	"fmt"
)

type validateFn func() error

func (c *Config) Validate(mode string) error {
	validations := []validateFn{
		notEmpty("mapping.path", c.Mapping.Path),
		inRange("tracing.sampling_rate", 0, 1, c.Tracing.SamplingRate),
	}

	switch mode {
	case "store":
		validations = append(validations, c.storeValidations()...)
	case "proxy":
		validations = append(validations, c.proxyValidations()...)
	case "single":
		validations = append(validations, c.proxyValidations()...)
		validations = append(validations, c.storeValidations()...)
	default:
		panic("unknown mode")
	}

	for _, fn := range validations {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) proxyValidations() []validateFn {
	return []validateFn{
		inRange("compression.docs_zstd_compression_level", -7, 22, c.Compression.DocsZstdCompressionLevel),
		inRange("compression.metas_zstd_compression_level", -7, 22, c.Compression.MetasZstdCompressionLevel),

		greaterThan("limits.query_rate", 0, c.Limits.QueryRate),
		greaterThan("limits.inflight_bulks", 0, c.Limits.InflightBulks),
		greaterThan("limits.doc_size", 0, c.Limits.DocSize),
	}
}

func (c *Config) storeValidations() []validateFn {
	validations := []validateFn{
		notEmpty("storage.data_dir", c.Storage.DataDir),
		greaterThan("storage.frac_size", 0, c.Storage.FracSize),
		greaterThan("storage.total_size", 0, c.Storage.TotalSize),

		greaterThan("limits.search_requests", 0, c.Limits.SearchRequests),
		greaterThan("limits.bulk_requests", 0, c.Limits.BulkRequests),
		greaterThan("limits.fraction_hits", 0, c.Limits.FractionHits),
		greaterThan("limits.search_docs", 0, c.Limits.SearchDocs),

		greaterThan("limits.aggregation.field_tokens", 0, c.Limits.Aggregation.FieldTokens),
		greaterThan("limits.aggregation.group_tokens", 0, c.Limits.Aggregation.GroupTokens),
		greaterThan("limits.aggregation.fraction_tokens", 0, c.Limits.Aggregation.FractionTokens),

		greaterThan("resources.reader_workers", 0, c.Resources.ReaderWorkers),
		greaterThan("resources.search_workers", 0, c.Resources.SearchWorkers),
		greaterThan("resources.replay_workers", 0, c.Resources.ReplayWorkers),
		greaterThan("resources.cache_size", 0, c.Resources.CacheSize),
		greaterThan("storage.sealing_queue_len", -1, c.Storage.SealingQueueLen),

		inRange("compression.sealed_zstd_compression_level", -7, 22, c.Compression.SealedZstdCompressionLevel),
		inRange("compression.doc_block_zstd_compression_level", -7, 22, c.Compression.DocBlockZstdCompressionLevel),
		greaterThan("sealing.lids.block_size", 0, c.Sealing.Lids.BlockSize),
		lessOrEqThan("sealing.lids.block_size", 65536, c.Sealing.Lids.BlockSize),
		greaterOrEqThan("sealing.lids.bitmap_threshold", 0, c.Sealing.Lids.BitmapThreshold),
		lessOrEqThan("sealing.lids.bitmap_threshold", c.Sealing.Lids.BlockSize, c.Sealing.Lids.BitmapThreshold),
		greaterThan("sealing.tokens.block_size", 0, c.Sealing.Tokens.BlockSize),
		greaterOrEqThan("sealing.tokens.freq_threshold_percentage", 0.0, c.Sealing.Tokens.FreqThresholdPercentage),
		lessOrEqThan("sealing.tokens.freq_threshold_percentage", 100.0, c.Sealing.Tokens.FreqThresholdPercentage),
		inRange("offloading.queue_size_percent", 0, 100, c.Offloading.QueueSizePercent),

		greaterThan("experimental.max_regex_tokens_check", -1, c.Experimental.MaxRegexTokensCheck),

		greaterThan("compaction.stcs.merge_trigger", 0, c.Compaction.STCS.MergeTrigger),
		greaterThan("compaction.stcs.merge_fan_out_size", 0, c.Compaction.STCS.MergeFanOutSize),
		greaterOrEqThan("compaction.stcs.merge_fan_in", c.Compaction.STCS.MergeTrigger, c.Compaction.STCS.MergeFanIn),

		greaterThan("compaction.stcs.bucket_lowerbound", 0, c.Compaction.STCS.BucketLowerbound),
		greaterOrEqThan("compaction.stcs.bucket_upperbound", c.Compaction.STCS.BucketLowerbound, c.Compaction.STCS.BucketUpperbound),

		greaterOrEqThan("compaction.workers", 0, c.Compaction.Workers),
		greaterThan("compaction.time_window", 0, c.Compaction.TimeWindow),
		greaterThan("compaction.tick_interval", 0, c.Compaction.TickInterval),
	}

	if c.Offloading.Enabled {
		validations = append(validations,
			notEmpty("offloading.bucket", c.Offloading.Bucket),
			notEmpty("offloading.access_key", c.Offloading.AccessKey),
			notEmpty("offloading.secret_key", c.Offloading.SecretKey),
		)
	}

	return validations
}

func notEmpty[T comparable](field string, v T) validateFn {
	return func() error {
		var z T
		if v == z {
			return fmt.Errorf("field %q is required", field)
		}
		return nil
	}
}

func greaterThan[T cmp.Ordered](field string, base, v T) validateFn {
	return func() error {
		if v <= base {
			return fmt.Errorf(
				"field %q must be greater than %v",
				field, base,
			)
		}
		return nil
	}
}

func lessOrEqThan[T cmp.Ordered](field string, base, v T) validateFn {
	return func() error {
		if v > base {
			return fmt.Errorf(
				"field %q must be less or equal than %v",
				field, base,
			)
		}
		return nil
	}
}

func greaterOrEqThan[T cmp.Ordered](field string, base, v T) validateFn {
	return func() error {
		if v < base {
			return fmt.Errorf(
				"field %q must be greater or equal than %v",
				field, base,
			)
		}
		return nil
	}
}

func inRange[T cmp.Ordered](field string, from, to, v T) validateFn {
	return func() error {
		if v < from || to < v {
			return fmt.Errorf(
				"field %q must be in range [%v; %v]",
				field, from, to,
			)
		}
		return nil
	}
}

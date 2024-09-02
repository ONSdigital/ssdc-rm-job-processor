# ssdc-rm-job-processor

The Job Processor handles the validation and processing of batches, for sample loading, bulk case refusal, bulk case invaldation, bulk case sample updadtes and bulk updates of sensitive PII data.

The Job Processor takes data loaded in CSV files via the Support Tool or Response Operations UI, validates and transforms that data, before shuttling it onto Google Pub/Sub topics, for processing in the Case Processor.

## How Does It Work?

The Job Processor is designed as a single-instance application, which polls the database looking for Jobs to process. Jobs can be either staged data which needs validating, or validated data which needs processing (transforming and sending to the Case Processor).

The reason it's a single instance is because there's no need to scale horizontally: the biggest files we would ever need to handle are Census sample files (30 million rows, approx)  and a single instance is more than capable of processing that amount of data in a reasonable timeframe.

There might be a desire in future to make the Job Processor better at handling concurrent Jobs, but for now, we expect that most Jobs are so small that users won't notice that there's an element of queueing happening.

## How Is It Structured?

The `StagedJobValidator` polls the database for Jobs which are in `VALIDATION_IN_PROGRESS` status, meaning that file staging is complete and the rows are in the database and ready to be validated.

When the `StagedJobValidator` finds a job, it checks to see if there are any JobRows with `STAGED`, meaning that the rows have been staged but not yet validated.

Having seen that there are some JobRows to validate, the `StagedJobValidator` asks the `RowChunkValidator` to validate a 'chunk' of rows... the chunking is done for performance and to keep transaction size down to something reasonable for huge batches.
If any JobRow has a validation problem, the Job will be marked as `VALIDATED_WITH_ERRORS` so that the user can see that there are problems with the CSV file.

Finally, once all the JobRows have been validated, the `StagedJobValidator` will mark the Job as `VALIDATED_OK` if there were no validation failures with any of the JobRows. There's a small bug here, where a restart of the Job Processor could theoretically cause a Job to be marked as OK when in reality it's actually got errors on some of the rows.

The `ValidatedJobProcessor` polls the database for Jobs which are in `PROCESSING_IN_PROGRESS` status, meaning that file validation is complete and the rows are be transformed and published to Case Processor.

When the `ValidatedJobProcessor` finds a job, it loops through processing chunks of rows until there are no more left, before finally setting the job state to be `PROCESSED` and deleting all the rows that it processed (but leaving any that failed validation to be downloaded by users in future).

The whole application is designed to be very extensible (see the `Transformer` interface and `JobTypeHelper`) so don't hack it too much if you're adding anything new!

## A Final Word on Performance

The batch sizes have been chosen quite arbitrarily, but the application should be able to process files of 30 million in under 8 hours, when given some decent hardware... start with beefing up the database to get the max IOPS, increase the RAM and CPU of the database... then throw some more resources at the Job Processor if it's still not as fast as you want.

Although it scales vertically not horizontally, the limiting factor is always going to be the number of transactions per second that the database can handle, so start by tuning that first.

There are a lot of configuration options available, so the author discourages code changes - start with the infrastructure, and then try config changes. It would seem highly unlikely that the design of the code would ever be the bottleneck, even for the biggest surveys that the ONS does (e.g. Census).

People might say "but I can load a CSV file in milliseconds"... yes, but not a very large one with robust protection against the process crashing part-way through. For sure, you can process small files in a single transaction very quickly, but once you get to large long-lived transactions, the problem is much harder.

If you need a robust and reliable production-strength solution, which you would entrust to do duties like Census.

Beware short-cuts which 'appear' to improve performance, but at the price of being flakey and unable to withstand a hardware failure or unexpected shutdown.
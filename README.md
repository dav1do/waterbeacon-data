# waterbeacon data collection

This is a POC / changes to [waterbeacon](https://github.com/codefordenver/waterbeacon) to use Azure services to collect and process data.

The strategy is to run a program (probably console app) that will query the data sources (EPA currently) and put the messages on a queue. There will be a serverless function triggered off the queue that will store the message in cosmosdb. Choosing to use a queue here because the (now) free tier of cosmosdb is 400 RUs, which would be overloaded if we tried to send everything in from the console app, so enqueueing/dequeuing is a simple mechanism to ensure it's all processed. Finally, there will be a function that exposes access to the cosmos database, though this could (should?) live elsewhere.

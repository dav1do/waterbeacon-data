const CosmosClient = require("@azure/cosmos").CosmosClient;

module.exports = async function (context, req) {
    context.log('Waterbeacon API receieved a request');
    let fips;

    if (req.query.fipscode || (req.body && req.body.fipscode)) {
        fips = req.query.fipscode || req.body.fipscode;
    }

    //TODO: don't want to retrieve all in general but especially without pagination i.e. no fetchAll()

    endpoint = process.env.cosmos_url;
    key = process.env.cosmos_key;
    databaseId = process.env.cosmos_db_name;
    containerId = process.env.cosmos_container_name;

    const client = new CosmosClient({ endpoint, key });
    const { database } = await client.databases.createIfNotExists({ id: databaseId });
    const { container } = await database.containers.createIfNotExists({ id: containerId });

    let queryRes;
    if (fips) {
        const querySpec = {
            query: "SELECT * FROM c WHERE  c.partition_key = @FipsCode",
            parameters: [
                {
                    name: "@FipsCode",
                    value: fips
                }
            ]
        };
        queryRes = await container.items.query(querySpec).fetchAll();
    }
    else {
        queryRes = await container.items.readAll().fetchAll();
    }
    context.log(`Cost: ${queryRes.requestCharge} with metrics: ${queryRes.queryMetrics}`);

    context.res = {
        status: 200,
        body: { "Facilities": queryRes.resources },
        headers: {
            'Content-Type': 'application/json'
        }
    }
    context.done();
};

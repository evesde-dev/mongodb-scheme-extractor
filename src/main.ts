import { MongoClient } from "mongodb";

export default class MongoDBSchemaExtractor {
    public static async generateSchema(
        connectionString: string,
        excludeCollections: string[] = [],
        excludeFields: string[] = [],
        recordCount: number = 50
    ): Promise<{ [key: string]: MongoDBSchema }> {
        const client = await MongoClient.connect(connectionString, {});
        const db = client.db();
        const schema: MongoDBSchema = {} as MongoDBSchema;
        //list collections
        for (const collectionInfo of await db
            .listCollections({ name: { $not: { $in: excludeCollections } } })
            .toArray()) {
            const collection = db.collection(collectionInfo.name);
            //get records
            const cursor = collection.find({}, { limit: recordCount });
            const properties: { name: string; type: string; count: number }[] =
                [];

            const records = await cursor.toArray();

            //find unique properties
            for (const record of records) {
                for (const [key, value] of Object.entries(record)) {
                    const pIndex = properties.findIndex((p) => p.name === key);
                    if (pIndex === -1) {
                        properties.push({
                            name: key,
                            type: typeof value,
                            count: 1,
                        });
                    } else {
                        properties[pIndex].count++;
                    }
                }
            }

            //build collection object
            const collectionSchema: MongoDBCollectionSchema =
                {} as MongoDBCollectionSchema;

            for (const prop of properties) {
                if (!excludeFields?.includes(prop.name))
                    collectionSchema[prop.name] = {
                        required: prop.count === records.length,
                        type: prop.type,
                    };
            }

            schema[collectionInfo.name] = collectionSchema;
        }
        //disconnect from database
        client.close();

        //find unique properties
        //output
        return { [db.databaseName]: schema };
    }
}

export interface MongoDBSchema {
    [key: string]: MongoDBCollectionSchema;
}

export interface MongoDBCollectionSchema {
    [key: string]: MongoDBSchemaProperty;
}

export interface MongoDBSchemaProperty {
    required: boolean;
    type: string;
}

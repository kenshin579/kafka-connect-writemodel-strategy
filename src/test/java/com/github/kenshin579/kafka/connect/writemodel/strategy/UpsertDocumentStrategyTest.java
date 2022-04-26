package com.github.kenshin579.kafka.connect.writemodel.strategy;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UpsertDocumentStrategyTest {

    public static final UpsertDocumentStrategy WRITE_STRATEGY = new UpsertDocumentStrategy();

    private static final BsonDocument VALUE_DOC =
            BsonDocument.parse(
                    "{_id: {a: {a1: 1}, b: {b1: 1, b2: 1}}, a: {a1: 1}, b: {b1: 1, b2: 1, c1: 1}}");


    @Test
    void testUpsertAsPartOfDocumentStrategy() {
//        assertNull(WRITE_STRATEGY.createWriteModel(new SinkDocument(new BsonDocument(), new BsonDocument())));

        WriteModel<BsonDocument> result =
                WRITE_STRATEGY.createWriteModel(new SinkDocument(null, VALUE_DOC.clone()));
        assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;
    }
}

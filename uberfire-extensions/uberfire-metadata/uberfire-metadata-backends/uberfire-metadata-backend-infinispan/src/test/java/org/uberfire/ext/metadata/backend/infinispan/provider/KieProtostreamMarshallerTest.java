package org.uberfire.ext.metadata.backend.infinispan.provider;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.infinispan.protostream.FileDescriptorSource.fromString;
import static org.junit.Assert.assertEquals;

public class KieProtostreamMarshallerTest {

    @Test
    public void testMultipleMarshallers() throws IOException, InterruptedException, ClassNotFoundException {
        KieProtostreamMarshaller marshaller = new KieProtostreamMarshaller();

        // Create a few dynamic entities with the same class, but different types.
        DynamicEntity1 entity1a = new DynamicEntity1(42, "value1");
        DynamicEntity1 entity1b = new DynamicEntity1(23, "other value");
        DynamicEntity1 entity1c = new DynamicEntity1(23, "yet value");

        // Create another dynamic entity (different class)
        DynamicEntity2 entity2 = new DynamicEntity2("org_infinispan_test", 1);

        // Register the protobuf files auto-generated from the dynamic entities
        marshaller.registerSchema("entity1a.proto", entity1a.getProto(), DynamicEntity1.class);
        marshaller.registerSchema("entity1b.proto", entity1b.getProto(), DynamicEntity1.class);
        // No need to generate for 'entity1c' since it shares the same type

        // Register protobuf schema for entity2
        marshaller.registerSchema("entity2.proto", entity2.getProto(), DynamicEntity2.class);

        // Register a marshaller provider for each dynamic entity class
        marshaller.registerMarshaller(new KieProtostreamMarshaller.KieMarshallerSupplier<DynamicEntity1>() {
            @Override
            public String extractTypeFromEntity(DynamicEntity1 entity) {
                return entity.getFullyQualifiedType();
            }

            @Override
            public Class<DynamicEntity1> getJavaClass() {
                return DynamicEntity1.class;
            }

            @Override
            public BaseMarshaller<DynamicEntity1> getMarshallerForType(String typeName) {
                return new DynamicEntity1Marshaller(typeName);
            }
        });

        marshaller.registerMarshaller(new KieProtostreamMarshaller.KieMarshallerSupplier<DynamicEntity2>() {
            @Override
            public String extractTypeFromEntity(DynamicEntity2 entity) {
                return entity.getType();
            }

            @Override
            public Class<DynamicEntity2> getJavaClass() {
                return DynamicEntity2.class;
            }

            @Override
            public BaseMarshaller<DynamicEntity2> getMarshallerForType(String typeName) {
                return new DynamicEntity2Marshaller(typeName);
            }
        });

        // Registers a marshaller based on a static entity (java.util.UUID)
        SerializationContext serCtx = marshaller.getSerializationContext();
        serCtx.registerProtoFiles(fromString("uuid.proto", "message unique_id { required string uuid=1; }"));
        serCtx.registerMarshaller(new UUIDMarshaller());

        // Marshall all dynamic entities
        byte[] bytes1a = marshaller.objectToByteBuffer(entity1a);
        byte[] bytes1b = marshaller.objectToByteBuffer(entity1b);
        byte[] bytes1c = marshaller.objectToByteBuffer(entity1c);
        byte[] bytes2 = marshaller.objectToByteBuffer(entity2);
        // Marshall a built-in type
        byte[] stringBytes = marshaller.objectToByteBuffer("Sample String");
        // Marshall a custom static object
        UUID uuid = UUID.randomUUID();
        byte[] uuidBytes = marshaller.objectToByteBuffer(uuid);

        // Unmarshalls all objects above
        DynamicEntity1 fromBytes1a = (DynamicEntity1) marshaller.objectFromByteBuffer(bytes1a);
        DynamicEntity1 fromBytes1b = (DynamicEntity1) marshaller.objectFromByteBuffer(bytes1b);
        DynamicEntity1 fromBytes1c = (DynamicEntity1) marshaller.objectFromByteBuffer(bytes1c);
        DynamicEntity2 fromBytes2 = (DynamicEntity2) marshaller.objectFromByteBuffer(bytes2);
        String stringFromBytes = (String) marshaller.objectFromByteBuffer(stringBytes);
        UUID uuidFromBytes = (UUID) marshaller.objectFromByteBuffer(uuidBytes);

        // Check everything is unmarshalled correctly
        assertEquals(entity1a, fromBytes1a);
        assertEquals(entity1b, fromBytes1b);
        assertEquals(entity1c, fromBytes1c);
        assertEquals(entity2, fromBytes2);
        assertEquals(stringFromBytes, "Sample String");
        assertEquals(uuidFromBytes, uuid);

        // Test in multi-threaded scenario
        List<Object> mixedEntities = new ArrayList<>(30);

        range(0, 50).boxed().forEach(i -> mixedEntities.add(new DynamicEntity1(42, "value" + i)));
        range(0, 70).boxed().forEach(i -> mixedEntities.add(new DynamicEntity1(23, "value" + i)));
        range(0, 80).boxed().forEach(i -> mixedEntities.add(new DynamicEntity2("org_infinispan_test", i)));

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        AtomicInteger failures = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            Set<Callable<Void>> tasks = mixedEntities.stream().map(o -> (Callable<Void>) () -> {
                try {
                    byte[] toBytes = marshaller.objectToByteBuffer(o);
                    Object back = marshaller.objectFromByteBuffer(toBytes);
                    if (!o.equals(back)) failures.incrementAndGet();
                    return null;
                } catch (IOException | InterruptedException | ClassNotFoundException e) {
                    failures.incrementAndGet();
                }
                return null;
            }).collect(Collectors.toSet());


            executorService.invokeAll(tasks);

            assertEquals(0, failures.get());
        }
    }

    class UUIDMarshaller implements MessageMarshaller<UUID> {
        @Override
        public UUID readFrom(ProtoStreamReader reader) throws IOException {
            return UUID.fromString(reader.readString("uuid"));
        }

        @Override
        public void writeTo(ProtoStreamWriter writer, UUID uuid) throws IOException {
            writer.writeString("uuid", uuid.toString());
        }

        @Override
        public Class<? extends UUID> getJavaClass() {
            return UUID.class;
        }

        @Override
        public String getTypeName() {
            return "unique_id";
        }
    }

    class DynamicEntity1 {
        private int seed;
        private String value;

        DynamicEntity1(int seed, String value) {
            this.seed = seed;
            this.value = value;
        }

        String getFullyQualifiedType() {
            return "type_" + seed;
        }

        String getProto() {
            return String.format("message %s { required int32 seed=1; required string value=2; }", getFullyQualifiedType());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DynamicEntity1 that = (DynamicEntity1) o;
            return seed == that.seed &&
                    Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seed, value);
        }

        @Override
        public String toString() {
            return "DynamicEntity1{" +
                    "seed=" + seed +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    class DynamicEntity2 {
        private String theType;
        private Integer rank;

        DynamicEntity2(String theType, Integer rank) {
            this.theType = theType;
            this.rank = rank;
        }

        String getType() {
            return theType;
        }

        String getProto() {
            return String.format("message %s { required string theType=1; required int32 rank=2; }", getType());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DynamicEntity2 that = (DynamicEntity2) o;
            return Objects.equals(theType, that.theType) &&
                    Objects.equals(rank, that.rank);
        }

        @Override
        public int hashCode() {
            return Objects.hash(theType, rank);
        }

        @Override
        public String toString() {
            return "DynamicEntity2{" +
                    "theType='" + theType + '\'' +
                    ", rank=" + rank +
                    '}';
        }
    }

    class DynamicEntity1Marshaller implements MessageMarshaller<DynamicEntity1> {

        private final String type;

        DynamicEntity1Marshaller(String type) {
            this.type = type;
        }

        @Override
        public DynamicEntity1 readFrom(ProtoStreamReader reader) throws IOException {
            Integer seed = reader.readInt("seed");
            String value = reader.readString("value");
            return new DynamicEntity1(seed, value);
        }

        @Override
        public void writeTo(ProtoStreamWriter writer, DynamicEntity1 obj) throws IOException {
            writer.writeInt("seed", obj.seed);
            writer.writeString("value", obj.value);
        }

        @Override
        public Class<? extends DynamicEntity1> getJavaClass() {
            return DynamicEntity1.class;
        }

        @Override
        public String getTypeName() {
            return type;
        }
    }

    class DynamicEntity2Marshaller implements MessageMarshaller<DynamicEntity2> {

        private final String type;

        DynamicEntity2Marshaller(String type) {
            this.type = type;
        }

        @Override
        public DynamicEntity2 readFrom(ProtoStreamReader reader) throws IOException {
            String type = reader.readString("theType");
            Integer rank = reader.readInt("rank");
            return new DynamicEntity2(type, rank);
        }

        @Override
        public void writeTo(ProtoStreamWriter writer, DynamicEntity2 obj) throws IOException {
            writer.writeString("theType", obj.theType);
            writer.writeInt("rank", obj.rank);
        }

        @Override
        public Class<? extends DynamicEntity2> getJavaClass() {
            return DynamicEntity2.class;
        }

        @Override
        public String getTypeName() {
            return type;
        }
    }

}
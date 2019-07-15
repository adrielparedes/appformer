package org.guvnor.structure.backend.organizationalunit.config;

import java.util.HashMap;
import java.util.Map;

import org.guvnor.structure.organizationalunit.config.SpaceConfigStorage;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorageBatch;
import org.guvnor.structure.organizationalunit.config.SpaceInfo;

public class ActiveSpaceConfigStorageBatchContextRegistry {

    private static Map<Long, SpaceConfigStorageBatchContextImpl> activeContexts = new HashMap<>();

    public static SpaceConfigStorageBatch.SpaceConfigStorageBatchContext getCurrentBatch(final SpaceConfigStorage spaceConfigStorage,
                                                                                         final SpaceConfigStorageBatch ownerBatch) {

        return activeContexts.computeIfAbsent(getContextId(), contextId -> new SpaceConfigStorageBatchContextImpl(spaceConfigStorage, ownerBatch));
    }

    public static void clearCurrentBatch() {
        activeContexts.remove(getContextId());
    }

    private static Long getContextId() {
        return Thread.currentThread().getId();
    }

    private static class SpaceConfigStorageBatchContextImpl implements SpaceConfigStorageBatch.SpaceConfigStorageBatchContext {

        private final SpaceConfigStorage spaceConfigStorage;
        private final Object owner;
        private SpaceInfo info;

        public SpaceConfigStorageBatchContextImpl(SpaceConfigStorage spaceConfigStorage, Object owner) {
            this.spaceConfigStorage = spaceConfigStorage;
            this.owner = owner;
        }

        @Override
        public SpaceInfo getSpaceInfo() {
            if (info == null) {
                info = spaceConfigStorage.loadSpaceInfo();
            }

            return info;
        }

        @Override
        public Object getOwner() {
            return owner;
        }

        @Override
        public void saveSpaceInfo() {
            spaceConfigStorage.saveSpaceInfo(info);
        }
    }
}

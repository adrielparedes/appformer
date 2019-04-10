/*
 * Copyright 2018 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.guvnor.structure.backend.repositories;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import javax.inject.Named;

import org.guvnor.structure.contributors.Contributor;
import org.guvnor.structure.organizationalunit.config.RepositoryInfo;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorage;
import org.guvnor.structure.repositories.Branch;
import org.guvnor.structure.repositories.Repository;
import org.guvnor.structure.repositories.RepositoryUpdatedEvent;
import org.guvnor.structure.server.config.ConfigGroup;
import org.guvnor.structure.server.config.ConfigItem;
import org.guvnor.structure.server.config.ConfigurationFactory;
import org.guvnor.structure.server.config.ConfigurationService;
import org.guvnor.structure.server.repositories.RepositoryFactory;
import org.uberfire.backend.server.util.Paths;
import org.uberfire.backend.vfs.Path;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.spaces.Space;

import static org.guvnor.structure.server.config.ConfigType.SPACE;

/**
 * Cache for configured repositories.
 * <p>
 * If you plan to use this outside of ProjectService make sure you know what you are doing.
 * <p>
 * It is safe to get data from this class, but any editing should be done through ProjectService.
 * Still if possible use ProjectService for accessing the repositories. It is part of a public API
 * and this is hidden in the -backend on purpose.
 */
@ApplicationScoped
public class ConfiguredRepositoriesImpl implements ConfiguredRepositories {

    private ConfigurationService configurationService;
    private RepositoryFactory repositoryFactory;
    private Repository systemRepository;
    private Event<RepositoryUpdatedEvent> repositoryUpdatedEvent;
    private ConfigurationFactory configurationFactory;
    private SpaceConfigStorage spaceConfigStorage;

    public ConfiguredRepositoriesImpl() {
    }

    @Inject
    public ConfiguredRepositoriesImpl(final ConfigurationService configurationService,
                                      final RepositoryFactory repositoryFactory,
                                      final @Named("system") Repository systemRepository,
                                      final Event<RepositoryUpdatedEvent> repositoryUpdatedEvent,
                                      final ConfigurationFactory configurationFactory,
                                      final SpaceConfigStorage spaceConfigStorage) {
        this.configurationService = configurationService;
        this.repositoryFactory = repositoryFactory;
        this.systemRepository = systemRepository;
        this.repositoryUpdatedEvent = repositoryUpdatedEvent;
        this.configurationFactory = configurationFactory;
        this.spaceConfigStorage = spaceConfigStorage;
    }

    private void checkIfRepositoryContributorsIsNotSet(final ConfigGroup repoConfig,
                                                       final Repository repository,
                                                       final Space space) {
        if (repoConfig.getConfigItem("contributors") == null) {
            final Optional<ConfigGroup> spaceConfig = configurationService.getConfiguration(SPACE).stream().filter(s -> s.getName().equals(space.getName())).findFirst();
            ConfigItem<List<Contributor>> spaceContributors = spaceConfig.get().getConfigItem("space-contributors");
            if (spaceContributors != null) {
                spaceContributors.getValue().forEach(c -> repository.getContributors().add(c));
                repoConfig.addConfigItem(configurationFactory.newConfigItem("contributors",
                                                                            repository.getContributors()));
                configurationService.updateConfiguration(repoConfig);
            }
        }
    }

    /**
     * @param space Space of the repository.
     * @param alias Name of the repository.
     * @return Repository that has a random branch as a root, usually master if master exists.
     */
    public Repository getRepositoryByRepositoryAlias(final Space space,
                                                     final String alias) {

        this.spaceConfigStorage.setup(space.getName());
        List<RepositoryInfo> repositories = this.spaceConfigStorage.loadSpaceInfo().getRepositories();

        return repositories.stream()
                .filter(this.getRepository(alias))
                .findAny()
                .map(repo -> repositoryFactory.newRepository(repo))
                .orElse(null);
    }

    private Predicate<RepositoryInfo> getRepository(String alias) {
        return (RepositoryInfo repositoryInfo) -> repositoryInfo.getName().equals(alias) && !repositoryInfo.isDeleted();
    }

    /**
     * This can also return System Repository.
     * @param fs
     * @return
     */
    //  TODO: no lo usooo
    public Repository getRepositoryByRepositoryFileSystem(FileSystem fs) {
//        if (fs == null) {
//            return null;
//        }
//
//        if (systemRepository.getDefaultBranch().isPresent()
//                && convert(systemRepository.getDefaultBranch().get().getPath()).getFileSystem().equals(fs)) {
//            return systemRepository;
//        }
//
//        for (ConfiguredRepositoriesBySpace configuredRepositoriesBySpace : repositoriesBySpace.values()) {
//            for (final Repository repository : configuredRepositoriesBySpace.getAllConfiguredRepositories()) {
//                if (repository.getDefaultBranch().isPresent()
//                        && convert(repository.getDefaultBranch().get().getPath()).getFileSystem().equals(fs)) {
//                    return repository;
//                }
//            }
//        }
        return null;
    }

    /**
     * @param space Space of the repository.
     * @param root Path to the repository root in any branch.
     * @return Repository root branch is still the default, usually master.
     */
    public Repository getRepositoryByRootPath(final Space space,
                                              final Path root) {
//        ConfiguredRepositoriesBySpace configuredRepositoriesBySpace = getConfiguredRepositoriesBySpace(space);
//        return configuredRepositoriesBySpace.get(root);
        return this.getAllConfiguredRepositories(space).stream().filter(r -> {
            if (r.getBranches() != null) {
                for (final Branch branch : r.getBranches()) {

                    Path rootPath = Paths.normalizePath(branch.getPath());
                    return root.equals(rootPath);
                }
                return false;
            } else {
                return false;
            }
        }).findFirst().orElse(null);
    }

    /**
     * @return Does not include system repository.
     */
    public List<Repository> getAllConfiguredRepositories(final Space space) {
        this.spaceConfigStorage.setup(space.getName());
        List<RepositoryInfo> repositories = this.spaceConfigStorage.loadSpaceInfo().getRepositories();

        return repositories.stream()
                .filter(r -> !r.isDeleted())
                .map(repo -> repositoryFactory.newRepository(repo))
                .collect(Collectors.toList());
    }

    public boolean containsAlias(final Space space,
                                 final String alias) {
        this.spaceConfigStorage.setup(space.getName());
        List<RepositoryInfo> repositories = this.spaceConfigStorage.loadSpaceInfo().getRepositories();
        return repositories.stream()
                .anyMatch(r -> !r.isDeleted() && r.getName().equals(alias)) &&
                SystemRepository.SYSTEM_REPO.getAlias().equals(alias);
    }
}

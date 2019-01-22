/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.guvnor.structure.backend.organizationalunit;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;

import org.guvnor.structure.contributors.Contributor;
import org.guvnor.structure.organizationalunit.NewOrganizationalUnitEvent;
import org.guvnor.structure.organizationalunit.OrganizationalUnit;
import org.guvnor.structure.organizationalunit.OrganizationalUnitService;
import org.guvnor.structure.organizationalunit.RemoveOrganizationalUnitEvent;
import org.guvnor.structure.organizationalunit.RepoAddedToOrganizationalUnitEvent;
import org.guvnor.structure.organizationalunit.RepoRemovedFromOrganizationalUnitEvent;
import org.guvnor.structure.organizationalunit.UpdatedOrganizationalUnitEvent;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorage;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorageRegistry;
import org.guvnor.structure.organizationalunit.config.SpaceInfo;
import org.guvnor.structure.repositories.Repository;
import org.guvnor.structure.repositories.RepositoryService;
import org.guvnor.structure.server.organizationalunit.OrganizationalUnitFactory;
import org.jboss.errai.bus.server.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.ext.security.management.api.event.UserDeletedEvent;
import org.uberfire.io.IOService;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.fs.jgit.JGitPathImpl;
import org.uberfire.rpc.SessionInfo;
import org.uberfire.security.authz.AuthorizationManager;
import org.uberfire.spaces.Space;
import org.uberfire.spaces.SpacesAPI;

@Service
@ApplicationScoped
public class OrganizationalUnitServiceImpl implements OrganizationalUnitService {

    private static final Logger logger = LoggerFactory.getLogger(OrganizationalUnitServiceImpl.class);

    private OrganizationalUnitFactory organizationalUnitFactory;

    private Event<NewOrganizationalUnitEvent> newOrganizationalUnitEvent;

    private Event<RemoveOrganizationalUnitEvent> removeOrganizationalUnitEvent;

    private Event<RepoAddedToOrganizationalUnitEvent> repoAddedToOrgUnitEvent;

    private Event<RepoRemovedFromOrganizationalUnitEvent> repoRemovedFromOrgUnitEvent;

    private Event<UpdatedOrganizationalUnitEvent> updatedOrganizationalUnitEvent;

    private AuthorizationManager authorizationManager;

    private SessionInfo sessionInfo;

    private SpacesAPI spaces;

    private RepositoryService repositoryService;

    private IOService ioService;

    private SpaceConfigStorageRegistry spaceConfigStorageRegistry;

    private FileSystem systemFS;

    public OrganizationalUnitServiceImpl() {
    }

    @Inject
    public OrganizationalUnitServiceImpl(final OrganizationalUnitFactory organizationalUnitFactory,
                                         final RepositoryService repositoryService,
                                         final Event<NewOrganizationalUnitEvent> newOrganizationalUnitEvent,
                                         final Event<RemoveOrganizationalUnitEvent> removeOrganizationalUnitEvent,
                                         final Event<RepoAddedToOrganizationalUnitEvent> repoAddedToOrgUnitEvent,
                                         final Event<RepoRemovedFromOrganizationalUnitEvent> repoRemovedFromOrgUnitEvent,
                                         final Event<UpdatedOrganizationalUnitEvent> updatedOrganizationalUnitEvent,
                                         final AuthorizationManager authorizationManager,
                                         final SpacesAPI spaces,
                                         final SessionInfo sessionInfo,
                                         @Named("ioStrategy") final IOService ioService,
                                         final SpaceConfigStorageRegistry spaceConfigStorageRegistry,
                                         final @Named("systemFS") FileSystem systemFS) {
        this.organizationalUnitFactory = organizationalUnitFactory;
        this.repositoryService = repositoryService;
        this.newOrganizationalUnitEvent = newOrganizationalUnitEvent;
        this.removeOrganizationalUnitEvent = removeOrganizationalUnitEvent;
        this.repoAddedToOrgUnitEvent = repoAddedToOrgUnitEvent;
        this.repoRemovedFromOrgUnitEvent = repoRemovedFromOrgUnitEvent;
        this.updatedOrganizationalUnitEvent = updatedOrganizationalUnitEvent;
        this.authorizationManager = authorizationManager;
        this.spaces = spaces;
        this.sessionInfo = sessionInfo;
        this.ioService = ioService;
        this.spaceConfigStorageRegistry = spaceConfigStorageRegistry;
        this.systemFS = systemFS;
    }

    // TODO MigrationSystemGit
    /*@PostConstruct
    public void loadOrganizationalUnits() {
        Collection<ConfigGroup> groups = configurationService.getConfiguration(ConfigType.SPACE);
        if (groups != null) {
            for (ConfigGroup groupConfig : groups) {
                // Make sure existing Organizational Units are correctly initialized with a default group id.
                String ouName = groupConfig.getName();
                String defaultGroupId = groupConfig.getConfigItemValue("defaultGroupId");
                if (defaultGroupId == null || defaultGroupId.trim().isEmpty()) {
                    groupConfig.setConfigItem(configurationFactory.newConfigItem("defaultGroupId",
                                                                                 getSanitizedDefaultGroupId(ouName)));
                    configurationService.updateConfiguration(groupConfig);
                }

                OrganizationalUnit ou = organizationalUnitFactory.newOrganizationalUnit(groupConfig);
                registeredOrganizationalUnits.put(ou.getName(),
                                                  ou);
            }
        }
        configuredRepositories.reloadRepositories();
    }*/

    public void userRemoved(final @Observes UserDeletedEvent event) {
        final String removedUserIdentifier = event.getIdentifier();
        for (OrganizationalUnit organizationalUnit : getAllOrganizationalUnits()) {
            final boolean userRemoved = organizationalUnit.getContributors().removeIf(c -> c.getUsername().equals(removedUserIdentifier));
            if (userRemoved) {
                updateOrganizationalUnit(organizationalUnit.getName(),
                                         organizationalUnit.getDefaultGroupId(),
                                         organizationalUnit.getContributors());
            }

            for (Repository repository : organizationalUnit.getRepositories()) {
                final List<Contributor> updatedRepositoryContributors = new ArrayList<>(repository.getContributors());
                final boolean repositoryContributorRemoved = updatedRepositoryContributors.removeIf(c -> c.getUsername().equals(removedUserIdentifier));
                if (repositoryContributorRemoved) {
                    repositoryService.updateContributors(repository,
                                                         updatedRepositoryContributors);
                }
            }
        }
    }

    @Override
    public OrganizationalUnit getOrganizationalUnit(final String name) {
        return organizationalUnitFactory.newOrganizationalUnit(spaceConfigStorageRegistry.get(name).loadSpaceInfo());
    }

    @Override
    public Collection<OrganizationalUnit> getAllOrganizationalUnits() {
        final List<OrganizationalUnit> spaces = new ArrayList<>();

        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(getNiogitPath())) {
            for (java.nio.file.Path spacePath : stream) {
                final File spaceDirectory = spacePath.toFile();
                if (spaceDirectory.isDirectory() && !spaceDirectory.getName().equals("system")) {
                    spaces.add(getOrganizationalUnit(spaceDirectory.getName()));
                }
            }

            return spaces;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Space> getAllUserSpaces() {
        return getAllOrganizationalUnits()
                .stream()
                .map(ou -> spaces.getSpace(ou.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public Collection<OrganizationalUnit> getOrganizationalUnits() {
        final List<OrganizationalUnit> result = new ArrayList<>();
        for (OrganizationalUnit ou : getAllOrganizationalUnits()) {
            if (authorizationManager.authorize(ou, sessionInfo.getIdentity())
                    || ou.getContributors().stream().anyMatch(c -> c.getUsername().equals(sessionInfo.getIdentity().getIdentifier()))) {
                result.add(ou);
            }
        }
        return result;
    }

    @Override
    public OrganizationalUnit createOrganizationalUnit(final String name,
                                                       final String defaultGroupId) {

        return createOrganizationalUnit(name,
                                        defaultGroupId,
                                        new ArrayList<>());
    }

    @Override
    public OrganizationalUnit createOrganizationalUnit(final String name,
                                                       final String defaultGroupId,
                                                       final Collection<Repository> repositories) {

        return createOrganizationalUnit(name,
                                        defaultGroupId,
                                        repositories,
                                        new ArrayList<>());
    }

    @Override
    public OrganizationalUnit createOrganizationalUnit(final String name,
                                                       final String defaultGroupId,
                                                       final Collection<Repository> repositories,
                                                       final Collection<Contributor> contributors) {
        if (spaceDirectoryExists(name)) {
            return null;
        }

        OrganizationalUnit newOrganizationalUnit = null;

        try {
            String _defaultGroupId = defaultGroupId == null || defaultGroupId.trim().isEmpty() ? getSanitizedDefaultGroupId(name) : defaultGroupId;
            final SpaceInfo spaceInfo = new SpaceInfo(name,
                                                      _defaultGroupId,
                                                      contributors,
                                                      getRepositoryAliases(repositories),
                                                      Collections.emptyList());
            spaceConfigStorageRegistry.get(name).saveSpaceInfo(spaceInfo);
            newOrganizationalUnit = organizationalUnitFactory.newOrganizationalUnit(spaceInfo);

            return newOrganizationalUnit;
        } finally {
            if (newOrganizationalUnit != null) {
                newOrganizationalUnitEvent.fire(new NewOrganizationalUnitEvent(newOrganizationalUnit,
                                                                               getUserInfo(sessionInfo)));
            }
        }
    }

    private List<String> getRepositoryAliases(final Collection<Repository> repositories) {
        final List<String> repositoryList = new ArrayList<>();
        for (Repository repo : repositories) {
            repositoryList.add(repo.getAlias());
        }
        return repositoryList;
    }

    @Override
    public OrganizationalUnit updateOrganizationalUnit(final String name,
                                                       final String defaultGroupId) {
        return updateOrganizationalUnit(name,
                                        defaultGroupId,
                                        null);
    }

    @Override
    public OrganizationalUnit updateOrganizationalUnit(String name,
                                                       String defaultGroupId,
                                                       Collection<Contributor> contributors) {
        final SpaceConfigStorage spaceConfigStorage = spaceConfigStorageRegistry.get(name);
        final SpaceInfo spaceInfo = spaceConfigStorage.loadSpaceInfo();

        if (spaceInfo != null) {
            OrganizationalUnit updatedOrganizationalUnit = null;
            try {
                // As per loadOrganizationalUnits(), all Organizational Units should have the default group id value set
                String _defaultGroupId = defaultGroupId == null || defaultGroupId.trim().isEmpty() ?
                        spaceInfo.getDefaultGroupId() : defaultGroupId;
                spaceInfo.setDefaultGroupId(_defaultGroupId);

                if (contributors != null) {
                    spaceInfo.setContributors(contributors);
                }

                spaceConfigStorage.saveSpaceInfo(spaceInfo);

                updatedOrganizationalUnit = getOrganizationalUnit(name);

                checkChildrenRepositoryContributors(updatedOrganizationalUnit);

                return updatedOrganizationalUnit;
            } finally {
                if (updatedOrganizationalUnit != null) {
                    updatedOrganizationalUnitEvent.fire(new UpdatedOrganizationalUnitEvent(updatedOrganizationalUnit,
                                                                                           getUserInfo(sessionInfo)));
                }
            }
        } else {
            throw new IllegalArgumentException("OrganizationalUnit " + name + " not found");
        }
    }

    private void checkChildrenRepositoryContributors(final OrganizationalUnit updatedOrganizationalUnit) {
        repositoryService.getAllRepositories(updatedOrganizationalUnit.getSpace()).forEach(repository -> {
            final List<Contributor> repositoryContributors = new ArrayList<>(repository.getContributors());
            final boolean repositoryContributorsChanged = repositoryContributors.retainAll(updatedOrganizationalUnit.getContributors());
            if (repositoryContributorsChanged) {
                repositoryService.updateContributors(repository, repositoryContributors);
            }
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void addRepository(final OrganizationalUnit organizationalUnit,
                              final Repository repository) {
        final SpaceConfigStorage spaceConfigStorage = spaceConfigStorageRegistry.get(organizationalUnit.getName());
        final SpaceInfo spaceInfo = spaceConfigStorage.loadSpaceInfo();

        if (spaceInfo != null) {
            try {
                spaceInfo.getRepositories().add(repository.getAlias());
                spaceConfigStorage.saveSpaceInfo(spaceInfo);
            } finally {
                repoAddedToOrgUnitEvent.fire(new RepoAddedToOrganizationalUnitEvent(organizationalUnit,
                                                                                    repository,
                                                                                    getUserInfo(sessionInfo)));
            }
        } else {
            throw new IllegalArgumentException("OrganizationalUnit " + organizationalUnit.getName() + " not found");
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void removeRepository(final OrganizationalUnit organizationalUnit,
                                 final Repository repository) {
        final SpaceConfigStorage spaceConfigStorage = spaceConfigStorageRegistry.get(organizationalUnit.getName());
        final SpaceInfo spaceInfo = spaceConfigStorage.loadSpaceInfo();

        if (spaceInfo != null) {
            try {
                spaceInfo.getRepositories().remove(repository.getAlias());
                spaceConfigStorage.saveSpaceInfo(spaceInfo);
            } finally {
                repoRemovedFromOrgUnitEvent.fire(new RepoRemovedFromOrganizationalUnitEvent(organizationalUnit,
                                                                                            repository,
                                                                                            getUserInfo(sessionInfo)));
            }
        } else {
            throw new IllegalArgumentException("OrganizationalUnit " + organizationalUnit.getName() + " not found");
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void addGroup(final OrganizationalUnit organizationalUnit,
                         final String group) {
        final SpaceConfigStorage spaceConfigStorage = spaceConfigStorageRegistry.get(organizationalUnit.getName());
        final SpaceInfo spaceInfo = spaceConfigStorage.loadSpaceInfo();

        if (spaceInfo != null) {
            OrganizationalUnit updatedOrganizationalUnit = null;
            try {
                spaceInfo.getSecurityGroups().add(group);
                spaceConfigStorage.saveSpaceInfo(spaceInfo);

                updatedOrganizationalUnit = getOrganizationalUnit(organizationalUnit.getName());
            } finally {
                if (updatedOrganizationalUnit != null) {
                    updatedOrganizationalUnitEvent.fire(new UpdatedOrganizationalUnitEvent(updatedOrganizationalUnit,
                                                                                           getUserInfo(sessionInfo)));
                }
            }
        } else {
            throw new IllegalArgumentException("OrganizationalUnit " + organizationalUnit.getName() + " not found");
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void removeGroup(final OrganizationalUnit organizationalUnit,
                            final String group) {
        final SpaceConfigStorage spaceConfigStorage = spaceConfigStorageRegistry.get(organizationalUnit.getName());
        final SpaceInfo spaceInfo = spaceConfigStorage.loadSpaceInfo();

        if (spaceInfo != null) {
            OrganizationalUnit updatedOrganizationalUnit = null;
            try {
                spaceInfo.getSecurityGroups().remove(group);
                spaceConfigStorage.saveSpaceInfo(spaceInfo);

                updatedOrganizationalUnit = getOrganizationalUnit(organizationalUnit.getName());
            } finally {
                if (updatedOrganizationalUnit != null) {
                    updatedOrganizationalUnitEvent.fire(new UpdatedOrganizationalUnitEvent(updatedOrganizationalUnit,
                                                                                           getUserInfo(sessionInfo)));
                }
            }
        } else {
            throw new IllegalArgumentException("OrganizationalUnit " + organizationalUnit.getName() + " not found");
        }
    }

    @Override
    public void removeOrganizationalUnit(String groupName) {
        final OrganizationalUnit organizationalUnit = getOrganizationalUnit(groupName);

        if (organizationalUnit != null) {
            repositoryService.removeRepositories(organizationalUnit.getSpace(),
                                                 organizationalUnit.getRepositories().stream().map(repo -> repo.getAlias()).collect(Collectors.toSet()));

            removeSpaceDirectory(organizationalUnit.getSpace());
            removeOrganizationalUnitEvent.fire(new RemoveOrganizationalUnitEvent(organizationalUnit,
                                                                                 getUserInfo(sessionInfo)));
        }
    }

    private void removeSpaceDirectory(final Space space) {
        final URI configPathURI = URI.create(SpacesAPI.resolveConfigFileSystemPath(SpacesAPI.Scheme.DEFAULT, space.getName()));

        final Path configPath = ioService.get(configPathURI);
        final JGitPathImpl configGitPath = (JGitPathImpl) configPath;
        final File spacePath = configGitPath.getFileSystem().getGit().getRepository().getDirectory().getParentFile().getParentFile();

        ioService.delete(configPath.getFileSystem().getPath(""));
        spacePath.delete();
    }

    @Override
    public OrganizationalUnit getParentOrganizationalUnit(final Repository repository) {
        for (final OrganizationalUnit organizationalUnit : getAllOrganizationalUnits()) {
            if (organizationalUnit.getRepositories() != null) {
                for (final Repository ouRepository : organizationalUnit.getRepositories()) {
                    if (ouRepository.getAlias().equals(repository.getAlias())) {
                        return organizationalUnit;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<OrganizationalUnit> getOrganizationalUnits(Repository repository) {
        final ArrayList<OrganizationalUnit> result = new ArrayList<>();

        for (final OrganizationalUnit organizationalUnit : getAllOrganizationalUnits()) {
            if (organizationalUnit.getRepositories() != null) {
                for (final Repository ouRepository : organizationalUnit.getRepositories()) {
                    if (ouRepository.getAlias().equals(repository.getAlias())) {
                        result.add(organizationalUnit);
                    }
                }
            }
        }

        return Collections.unmodifiableList(result);
    }

    @Override
    public String getSanitizedDefaultGroupId(final String proposedGroupId) {
        //Only [A-Za-z0-9_\-.] are valid so strip everything else out
        return proposedGroupId != null ? proposedGroupId.replaceAll("[^A-Za-z0-9_\\-.]",
                                                                    "") : proposedGroupId;
    }

    @Override
    public Boolean isValidGroupId(final String proposedGroupId) {
        if (proposedGroupId != null && !proposedGroupId.trim().isEmpty()) {
            if (proposedGroupId.length() == getSanitizedDefaultGroupId(proposedGroupId).length()) {
                return true;
            }
        }
        return false;
    }

    protected String getUserInfo(SessionInfo sessionInfo) {
        try {
            return sessionInfo.getIdentity().getIdentifier();
        } catch (final Exception e) {
            return "system";
        }
    }

    private java.nio.file.Path getNiogitPath() {
        final JGitPathImpl systemGitPath = (JGitPathImpl) systemFS.getPath("system");
        return systemGitPath.getFileSystem().getGit().getRepository().getDirectory().getParentFile().getParentFile().toPath();
    }

    private boolean spaceDirectoryExists(String spaceName) {
        return getNiogitPath().resolve(spaceName).toFile().exists();
    }
}

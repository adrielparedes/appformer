package org.guvnor.structure.backend.repositories;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.guvnor.structure.contributors.Contributor;
import org.guvnor.structure.contributors.ContributorType;
import org.guvnor.structure.organizationalunit.config.RepositoryConfiguration;
import org.guvnor.structure.organizationalunit.config.RepositoryInfo;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorage;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorageRegistry;
import org.guvnor.structure.organizationalunit.config.SpaceInfo;
import org.guvnor.structure.repositories.Branch;
import org.guvnor.structure.repositories.Repository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.uberfire.spaces.Space;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RepositoryServiceImplTest {

    @Mock
    private Repository repository;

    @Mock
    private ConfiguredRepositories configuredRepositories;

    @Mock
    private SpaceConfigStorageRegistry registry;

    @InjectMocks
    private RepositoryServiceImpl repositoryService;

    @Test
    public void testNotCreateNewAliasIfNecessary() {
        when(configuredRepositories.getRepositoryByRepositoryAlias(any(),
                                                                   eq("other-name"))).thenReturn(repository);
        doReturn(Optional.of(mock(Branch.class))).when(repository).getDefaultBranch();
        doReturn("alias").when(repository).getAlias();
        String newAlias = repositoryService.createFreshRepositoryAlias("alias",
                                                                       new Space("alias"));

        assertEquals("alias",
                     newAlias);
    }

    @Test
    public void testCreateNewAliasIfNecessary() {
        when(configuredRepositories.getRepositoryByRepositoryAlias(any(),
                                                                   eq("alias"),
                                                                   eq(true))).thenReturn(repository);
        doReturn(Optional.of(mock(Branch.class))).when(repository).getDefaultBranch();
        doReturn("alias").when(repository).getAlias();
        String newAlias = repositoryService.createFreshRepositoryAlias("alias",
                                                                       new Space("alias"));

        assertEquals("alias-1",
                     newAlias);
    }

    @Test
    public void testCreateSecondNewAliasIfNecessary() {
        when(configuredRepositories.getRepositoryByRepositoryAlias(any(),
                                                                   eq("alias"),
                                                                   eq(true))).thenReturn(repository);
        when(configuredRepositories.getRepositoryByRepositoryAlias(any(),
                                                                   eq("alias-1"),
                                                                   eq(true))).thenReturn(repository);
        doReturn(Optional.of(mock(Branch.class))).when(repository).getDefaultBranch();
        doReturn("alias").when(repository).getAlias();
        String newAlias = repositoryService.createFreshRepositoryAlias("alias",
                                                                       new Space("alias"));

        assertEquals("alias-2",
                     newAlias);
    }

    @Test
    public void updateContributorsTest() {

        final Space space = new Space("alias");
        doReturn(space).when(repository).getSpace();
        doReturn("alias").when(repository).getAlias();

        doAnswer(invocationOnMock -> {
            final SpaceConfigStorage spaceConfigStorage = mock(SpaceConfigStorage.class);
            doReturn(new SpaceInfo((String) invocationOnMock.getArguments()[0],
                                   false,
                                   "defaultGroupId",
                                   Collections.emptyList(),
                                   new ArrayList<>(Arrays.asList(new RepositoryInfo("alias",
                                                                                    false,
                                                                                    new RepositoryConfiguration()))),
                                   Collections.emptyList())).when(spaceConfigStorage).loadSpaceInfo();
            doReturn(true)
                    .when(spaceConfigStorage).isInitialized();
            return spaceConfigStorage;
        }).when(registry).get(any());

        repositoryService.updateContributors(repository,
                                             Collections.singletonList(new Contributor("admin1",
                                                                                       ContributorType.OWNER)));

//        verify(configurationService).updateConfiguration(configGroup);
    }
}
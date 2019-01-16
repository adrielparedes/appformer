package org.guvnor.structure.backend;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.jgit.util.FileUtils;
import org.guvnor.structure.organizationalunit.OrganizationalUnit;
import org.guvnor.structure.organizationalunit.OrganizationalUnitService;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorage;
import org.guvnor.structure.organizationalunit.config.SpaceConfigStorageRegistry;
import org.guvnor.structure.organizationalunit.config.SpaceInfo;
import org.guvnor.structure.repositories.Branch;
import org.guvnor.structure.repositories.Repository;
import org.guvnor.structure.repositories.RepositoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.io.IOService;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.fs.jgit.FileSystemLock;
import org.uberfire.java.nio.fs.jgit.FileSystemLockManager;
import org.uberfire.java.nio.fs.jgit.JGitPathImpl;
import org.uberfire.spaces.Space;
import org.uberfire.spaces.SpacesAPI;

import static java.util.stream.Collectors.toList;
import static org.uberfire.backend.server.util.Paths.convert;

@Singleton
@Startup
public class FileSystemDeleteWorker {

    private static final int LAST_ACCESS_THRESHOLD = 10;
    private static final TimeUnit LAST_ACCESS_TIME_UNIT = TimeUnit.SECONDS;
    private static final String LOCK_NAME = "delete.lock";
    public static final String CRON_MINUTES = "*/1";

    private Logger logger = LoggerFactory.getLogger(FileSystemDeleteWorker.class);

    private IOService ioService;
    private OrganizationalUnitService organizationalUnitService;
    private RepositoryService repositoryService;
    private FileSystem systemFS;
    private SpaceConfigStorageRegistry registry;
    private boolean busy = false;

    public FileSystemDeleteWorker() {

    }

    @Inject
    public FileSystemDeleteWorker(@Named("ioStrategy") final IOService ioService,
                                  final OrganizationalUnitService organizationalUnitService,
                                  final RepositoryService repositoryService,
                                  final @Named("systemFS") FileSystem systemFS,
                                  final SpaceConfigStorageRegistry registry) {
        this.ioService = ioService;
        this.organizationalUnitService = organizationalUnitService;
        this.repositoryService = repositoryService;
        this.systemFS = systemFS;
        this.registry = registry;
    }

    @Schedule(hour = "*", minute = CRON_MINUTES, persistent = false)
    public void doRemove() {

        if (this.busy) {
            return;
        }
        this.busy = true;
        ifDebugEnabled(logger,
                       () -> logger.debug("Trying to acquire lock"));
        this.lockedOperation(() -> {
            ifDebugEnabled(logger,
                           () -> logger.debug("Lock acquired, executing Delete Operation"));
            this.removeAllDeletedSpaces();
            this.removeAllDeletedRepositories();
        });
        ifDebugEnabled(logger,
                       () -> logger.debug("Delete Operation finished."));
        this.busy = false;
    }

    protected void removeAllDeletedRepositories() {
        try {
            ifDebugEnabled(logger,
                           () -> logger.debug("Removing all deleted repositories"));
            Collection<OrganizationalUnit> spaces = this.organizationalUnitService.getAllOrganizationalUnits();
            List<Repository> deletedRepositories = spaces.stream()
                    .filter(organizationalUnit -> organizationalUnit != null)
                    .map(organizationalUnit ->
                                 this.repositoryService.getAllDeletedRepositories(organizationalUnit.getSpace()))
                    .flatMap(x -> x.stream()).collect(toList());

            ifDebugEnabled(logger,
                           () -> logger.debug("Found {} spaces with deleted repositories",
                                              deletedRepositories.size()));

            deletedRepositories
                    .forEach(organizationalUnit ->
                                     this.removeRepository(organizationalUnit));

            ifDebugEnabled(logger,
                           () -> logger.debug("Deleted repositories had been removed"));
        } catch (Exception e) {
            ifDebugEnabled(logger,
                           () -> logger.error("Error when trying to remove all deleted repositories",
                                              e));
        }
    }

    protected void removeAllDeletedSpaces() {
        try {
            ifDebugEnabled(logger,
                           () -> logger.debug("Removing all deleted spaces"));
            Collection<OrganizationalUnit> deletedSpaces = this.organizationalUnitService.getAllDeletedOrganizationalUnit();
            ifDebugEnabled(logger,
                           () -> logger.debug("Found {} spaces to be deleted",
                                              deletedSpaces.size()));
            deletedSpaces.forEach(ou -> this.removeSpaceDirectory(ou.getSpace()));
            ifDebugEnabled(logger,
                           () -> logger.debug("Deleted spaces had been removed"));
        } catch (Exception e) {
            ifDebugEnabled(logger,
                           () -> logger.error("Error when trying to remove all deleted Spaces",
                                              e));
        }
    }

    protected void removeSpaceDirectory(final Space space) {
        try {

            Collection<Repository> repositories = this.repositoryService.getAllRepositories(space,
                                                                                            true);

            repositories.forEach(repository -> this.removeRepository(repository));

            final URI configPathURI = getConfigPathUri(space);
            final Path configPath = ioService.get(configPathURI);
            final File spacePath = getSpacePath((JGitPathImpl) configPath);

            this.ioService.deleteIfExists(configPath.getFileSystem().getPath(""));
            this.delete(spacePath);
            this.registry.remove(space.getName());
        } catch (Exception e) {
            ifDebugEnabled(logger,
                           () -> logger.error("A problem occurred when trying to delete " + space.getName() + " space",
                                              e));
        }
    }

    protected void delete(File path) throws IOException {
        FileUtils.delete(path,
                         FileUtils.RECURSIVE | FileUtils.SKIP_MISSING | FileUtils.RETRY);
    }

    protected File getSpacePath(JGitPathImpl configPath) {
        final JGitPathImpl configGitPath = configPath;
        return configGitPath.getFileSystem()
                .getGit()
                .getRepository()
                .getDirectory()     // system.git
                .getParentFile()    // system
                .getParentFile();   //.niogit
    }

    private URI getConfigPathUri(Space space) {
        return URI.create(SpacesAPI.resolveConfigFileSystemPath(SpacesAPI.Scheme.DEFAULT,
                                                                space.getName()));
    }

    protected void removeRepository(final Repository repo) {
        try {
            Path path = getPath(repo);
            ioService.delete(path);
            if (!ioService.exists(path)) {
                this.removeRepositoryFromSpaceInfo(repo);
            }
        } catch (Exception e) {
            ifDebugEnabled(logger,
                           () -> logger.error("A problem occurred when trying to delete " + repo.getAlias() + " repository",
                                              e));
        }
    }

    private Path getPath(Repository repo) {
        Branch defaultBranch = repo.getDefaultBranch().orElseThrow(() -> new IllegalStateException("Repository should have at least one branch."));
        return convert(defaultBranch.getPath()).getFileSystem().getPath(null);
    }

    private void removeRepositoryFromSpaceInfo(Repository repo) {
        SpaceConfigStorage spaceConfigStorage = this.registry.get(repo.getSpace().getName());
        SpaceInfo spaceInfo = this.registry.get(repo.getSpace().getName()).loadSpaceInfo();
        spaceInfo.removeRepository(repo.getAlias());
        spaceConfigStorage.saveSpaceInfo(spaceInfo);
    }

    private File getSystemRepository() {
        return ((JGitPathImpl) systemFS.getPath("system"))
                .getFileSystem()
                .getGit()
                .getRepository()
                .getDirectory();
    }

    private void lockedOperation(Runnable runnable) {
        FileSystemLock physicalLock = createLock(this.getSystemRepository().getParentFile().getParentFile());
        try {
            physicalLock.lock();
            runnable.run();
        } finally {
            physicalLock.unlock();
        }
    }

    protected FileSystemLock createLock(File file) {
        ifDebugEnabled(logger,
                       () -> logger.debug("Acquiring lock: " + file.getAbsolutePath() + " - " + LOCK_NAME));
        return FileSystemLockManager
                .getInstance()
                .getFileSystemLock(file.getParentFile(),
                                   LOCK_NAME,
                                   LAST_ACCESS_TIME_UNIT,
                                   LAST_ACCESS_THRESHOLD);
    }

    private void ifDebugEnabled(Logger logger,
                                Runnable message) {
        if (logger.isDebugEnabled()) {
            message.run();
        }
    }
}

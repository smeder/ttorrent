package com.turn.ttorrent.tracker;

import com.turn.ttorrent.common.Torrent;
import jargs.gnu.CmdLineParser;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * BitTorrent tracker.
 *
 * <p>
 * The tracker usually listens on port 6969 (the standard BitTorrent tracker
 * port). Torrents must be registered directly to this tracker with the
 * {@link #announce(TrackedTorrent torrent)}</code> method.
 * </p>
 *
 * @author mpetazzoni
 */
public class HubTracker {

    private static final Logger logger =
            LoggerFactory.getLogger(HubTracker.class);

    /** Request path handled by the tracker announce request handler. */
    public static final String ANNOUNCE_URL = "/announce";

    /** Default tracker listening port (BitTorrent's default is 6969). */
    public static final int DEFAULT_TRACKER_PORT = 6969;

    /** Default server name and version announced by the tracker. */
    public static final String DEFAULT_VERSION_STRING =
            "BitTorrent Tracker (ttorrent)";

    private final Connection connection;
    private final InetSocketAddress address;

    /** The in-memory repository of torrents tracked. */
    private final ConcurrentMap<String, TrackedTorrent> torrents;

    private Thread tracker;
    private Thread collector;
    private Thread directoryMonitor;
    private boolean stop;
    private String inputDirectory;
    private String torrentDirectory;

    /**
     * Create a new BitTorrent tracker listening at the given address on the
     * default port.
     *
     * @param address The address to bind to.
     * @throws IOException Throws an <em>IOException</em> if the tracker
     * cannot be initialized.
     */
    public HubTracker(InetAddress address, String inputDirectory, String torrentDirectory) throws IOException {
        this(new InetSocketAddress(address, DEFAULT_TRACKER_PORT),
                DEFAULT_VERSION_STRING, inputDirectory, torrentDirectory);
    }

    /**
     * Create a new BitTorrent tracker listening at the given address.
     *
     * @param address The address to bind to.
     * @throws IOException Throws an <em>IOException</em> if the tracker
     * cannot be initialized.
     */
    public HubTracker(InetSocketAddress address, String inputDirectory, String torrentDirectory) throws IOException {
        this(address, DEFAULT_VERSION_STRING, inputDirectory, torrentDirectory);
    }

    /**
     * Create a new BitTorrent tracker listening at the given address.
     *
     * @param address The address to bind to.
     * @param version A version string served in the HTTP headers
     * @throws IOException Throws an <em>IOException</em> if the tracker
     * cannot be initialized.
     */
    public HubTracker(InetSocketAddress address, String version, String inputDirectory, String torrentDirectory)
            throws IOException {
        this.address = address;
        this.inputDirectory = inputDirectory;
        this.torrentDirectory = torrentDirectory;

        this.torrents = new ConcurrentHashMap<String, TrackedTorrent>();
        this.connection = new SocketConnection(
                new TrackerService(version, this.torrents));
    }

    /**
     * Returns the full announce URL served by this tracker.
     *
     * <p>
     * This has the form http://host:port/announce.
     * </p>
     */
    public URL getAnnounceUrl() {
        try {
            return new URL("http",
                    this.address.getAddress().getCanonicalHostName(),
                    this.address.getPort(),
                    HubTracker.ANNOUNCE_URL);
        } catch (MalformedURLException mue) {
            logger.error("Could not build tracker URL: {}!", mue, mue);
        }

        return null;
    }

    /**
     * Start the tracker thread.
     */
    public void start() {
        if (this.tracker == null || !this.tracker.isAlive()) {
            this.tracker = new TrackerThread();
            this.tracker.setName("tracker:" + this.address.getPort());
            this.tracker.start();
        }

        if (this.collector == null || !this.collector.isAlive()) {
            this.collector = new PeerCollectorThread();
            this.collector.setName("peer-collector:" + this.address.getPort());
            this.collector.start();
        }
        if (this.directoryMonitor == null || !this.directoryMonitor.isAlive()) {
            this.directoryMonitor = new Thread(new NewTorrentMonitorThread(inputDirectory, torrentDirectory, this));
            this.directoryMonitor.setName("directory-monitor: " + inputDirectory);
            this.directoryMonitor.start();
        }
    }

    /**
     * Stop the tracker.
     *
     * <p>
     * This effectively closes the listening HTTP connection to terminate
     * the service, and interrupts the peer collector thread as well.
     * </p>
     */
    public void stop() {
        this.stop = true;

        try {
            this.connection.close();
            logger.info("BitTorrent tracker closed.");
        } catch (IOException ioe) {
            logger.error("Could not stop the tracker: {}!", ioe.getMessage());
        }

        if (this.collector != null && this.collector.isAlive()) {
            this.collector.interrupt();
            logger.info("Peer collection terminated.");
        }

        if (this.directoryMonitor != null && this.directoryMonitor.isAlive()) {
            this.directoryMonitor.interrupt();
            logger.info("Directory monitor terminated");
        }
    }

    /**
     * Announce a new torrent on this tracker.
     *
     * <p>
     * The fact that torrents must be announced here first makes this tracker a
     * closed BitTorrent tracker: it will only accept clients for torrents it
     * knows about, and this list of torrents is managed by the program
     * instrumenting this Tracker class.
     * </p>
     *
     * @param torrent The Torrent object to start tracking.
     * @return The torrent object for this torrent on this tracker. This may be
     * different from the supplied Torrent object if the tracker already
     * contained a torrent with the same hash.
     */
    public synchronized TrackedTorrent announce(TrackedTorrent torrent) {
        TrackedTorrent existing = this.torrents.get(torrent.getHexInfoHash());

        if (existing != null) {
            logger.warn("Tracker already announced torrent for '{}' " +
                    "with hash {}.", existing.getName(), existing.getHexInfoHash());
            return existing;
        }

        this.torrents.put(torrent.getHexInfoHash(), torrent);
        logger.info("Registered new torrent for '{}' with hash {}.",
                torrent.getName(), torrent.getHexInfoHash());
        return torrent;
    }

    /**
     * Stop announcing the given torrent.
     *
     * @param torrent The Torrent object to stop tracking.
     */
    public synchronized void remove(Torrent torrent) {
        if (torrent == null) {
            return;
        }

        this.torrents.remove(torrent.getHexInfoHash());
    }

    /**
     * Stop announcing the given torrent after a delay.
     *
     * @param torrent The Torrent object to stop tracking.
     * @param delay The delay, in milliseconds, before removing the torrent.
     */
    public synchronized void remove(Torrent torrent, long delay) {
        if (torrent == null) {
            return;
        }

        new Timer().schedule(new TorrentRemoveTimer(this, torrent), delay);
    }

    /**
     * Timer task for removing a torrent from a tracker.
     *
     * <p>
     * This task can be used to stop announcing a torrent after a certain delay
     * through a Timer.
     * </p>
     */
    private static class TorrentRemoveTimer extends TimerTask {

        private HubTracker tracker;
        private Torrent torrent;

        TorrentRemoveTimer(HubTracker tracker, Torrent torrent) {
            this.tracker = tracker;
            this.torrent = torrent;
        }

        @Override
        public void run() {
            this.tracker.remove(torrent);
        }
    }

    /**
     * The main tracker thread.
     *
     * <p>
     * The core of the BitTorrent tracker run by the controller is the
     * SimpleFramework HTTP service listening on the configured address. It can
     * be stopped with the <em>stop()</em> method, which closes the listening
     * socket.
     * </p>
     */
    private class TrackerThread extends Thread {

        @Override
        public void run() {
            logger.info("Starting BitTorrent tracker on {}...",
                    getAnnounceUrl());

            try {
                connection.connect(address);
            } catch (IOException ioe) {
                logger.error("Could not start the tracker: {}!", ioe.getMessage());
                HubTracker.this.stop();
            }
        }
    }

    /**
     * The unfresh peer collector thread.
     *
     * <p>
     * Every PEER_COLLECTION_FREQUENCY_SECONDS, this thread will collect
     * unfresh peers from all announced torrents.
     * </p>
     */
    private class PeerCollectorThread extends Thread {

        private static final int PEER_COLLECTION_FREQUENCY_SECONDS = 15;

        @Override
        public void run() {
            logger.info("Starting tracker peer collection for tracker at {}...",
                    getAnnounceUrl());

            while (!stop) {
                for (TrackedTorrent torrent : torrents.values()) {
                    torrent.collectUnfreshPeers();
                }

                try {
                    Thread.sleep(PeerCollectorThread
                            .PEER_COLLECTION_FREQUENCY_SECONDS * 1000);
                } catch (InterruptedException ie) {
                    // Ignore
                }
            }
        }
    }

    public class NewTorrentMonitorThread implements Runnable {
        private final Path inputDir;
        private final File torrentDir;
        private final String trackerHostname;
        private final HubTracker tracker;

        public NewTorrentMonitorThread(String inputDir, String torrentDir, HubTracker tracker) {
            this.inputDir = FileSystems.getDefault().getPath(inputDir);
            this.torrentDir = new File(torrentDir);
            this.tracker = tracker;
            List<String> ips = new ArrayList<String>();
            List<String> hostnames = new ArrayList<String>();
            try {
                scanNics(ips, hostnames);
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
            if (hostnames.isEmpty()) {
                if (ips.isEmpty()) {
                    throw new RuntimeException("Couldn't determin IP or hostname!");
                }
                trackerHostname = ips.get(0);
            } else {
                trackerHostname = hostnames.get(0);
            }
        }

        private URI getTrackerURI() {
            try {
                return new URI("http://" + trackerHostname + ":" + tracker.address.getPort() + ANNOUNCE_URL);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        private void scanNics(List<String> ipAddrs, List<String> hostnames) throws SocketException {

            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces != null && networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();
                Enumeration<InetAddress> ina = ni.getInetAddresses();
                while (ina != null && ina.hasMoreElements()) {
                    InetAddress a = ina.nextElement();
                    if (a.isAnyLocalAddress()
                            || a.isLoopbackAddress()
                            || a.isLinkLocalAddress()) {
                        continue;
                    }
                    ipAddrs.add(a.getHostAddress());
                    if (a.getHostName() != null && a.getHostName().length() > 0) {
                        if (!a.getHostAddress().equals(a.getHostName())) {
                            hostnames.add(a.getHostName());
                        }
                    }
                }
            }
        }

        @Override
        public void run() {
            try {
                WatchService watcher = FileSystems.getDefault().newWatchService();
                inputDir.register(watcher,
                        ENTRY_CREATE,
                        ENTRY_DELETE,
                        ENTRY_MODIFY);

                for (;;) {
                    // wait for key to be signaled
                    WatchKey key;
                    try {
                        key = watcher.take();
                    } catch (InterruptedException x) {
                        return;
                    }

                    for (WatchEvent<?> event: key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();

                        // This key is registered only
                        // for ENTRY_CREATE events,
                        // but an OVERFLOW event can
                        // occur regardless if events
                        // are lost or discarded.
                        if (kind == OVERFLOW) {
                            continue;
                        }

                        // The filename is the
                        // context of the event.
                        WatchEvent<Path> ev = (WatchEvent<Path>)event;
                        Path filename = ev.context();

                        if (kind == ENTRY_CREATE) {
                            String creator = String.format("%s (ttorrent)",
                                    System.getProperty("user.name"));
                            FileOutputStream outputStream = null;
                            try {
                                Torrent torrent = Torrent.create(new File(inputDirectory, filename.toString()), getTrackerURI(), creator);
                                outputStream = new FileOutputStream(new File(torrentDir, filename + ".torrent"));
                                torrent.save(new BufferedOutputStream(outputStream));
                                tracker.announce(new TrackedTorrent(torrent));
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                                System.exit(1);
                            } catch (InterruptedException e) {
                                return;
                            } finally {
                                if (outputStream != null) {
                                    outputStream.close();
                                }
                            }
                        }
                    }

                    // Reset the key -- this step is critical if you want to
                    // receive further watch events.  If the key is no longer valid,
                    // the directory is inaccessible so exit the loop.
                    boolean valid = key.reset();
                    if (!valid) {
                        System.exit(1);
                    }
                }
            } catch (IOException x) {
                x.printStackTrace();
                System.exit(1);
            }
        }
    }

    /**
     * Display program usage on the given {@link PrintStream}.
     */
    private static void usage(PrintStream s) {
        s.println("usage: Tracker [options] inputDirectory torrentDirectory");
        s.println();
        s.println("Available options:");
        s.println("  -h,--help             Show this help and exit.");
        s.println("  -p,--port PORT        Bind to port PORT.");
        s.println();
    }

    /**
     * Main function to start a tracker.
     */
    public static void main(String[] args) {
        BasicConfigurator.configure(new ConsoleAppender(
                new PatternLayout("%d [%-25t] %-5p: %m%n")));

        CmdLineParser parser = new CmdLineParser();
        CmdLineParser.Option help = parser.addBooleanOption('h', "help");
        CmdLineParser.Option port = parser.addIntegerOption('p', "port");

        try {
            parser.parse(args);
        } catch (CmdLineParser.OptionException oe) {
            System.err.println(oe.getMessage());
            usage(System.err);
            System.exit(1);
        }

        // Display help and exit if requested
        if (Boolean.TRUE.equals(parser.getOptionValue(help))) {
            usage(System.out);
            System.exit(0);
        }

        Integer portValue = (Integer)parser.getOptionValue(port,
                Integer.valueOf(DEFAULT_TRACKER_PORT));

        String[] otherArgs = parser.getRemainingArgs();

        if (otherArgs.length != 2) {
            usage(System.err);
            System.exit(1);
        }

        // Get directories from command-line arguments
        String inputDirectory = otherArgs[0];
        String torrentDirectory = otherArgs[1];


        try {
            HubTracker t = new HubTracker(new InetSocketAddress(portValue.intValue()), inputDirectory, torrentDirectory);

            logger.info("Starting tracker with {} announced torrents...",
                    t.torrents.size());
            t.start();
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
            System.exit(2);
        }
    }

}

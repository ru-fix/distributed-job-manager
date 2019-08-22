package ru.fix.distributed.job.manager.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Simple printer of Zookeeper node tree.
 * The implementation depends of Apache Curator Framework.
 */
public class ZkTreePrinter {

    private static final Logger log = LoggerFactory.getLogger(ZkTreePrinter.class);

    private final CuratorFramework client;

    public ZkTreePrinter(CuratorFramework client) {
        this.client = client;
    }

    /**
     * node's recursive printer
     *
     * @param path node path
     * @return tree's string representation
     */
    public String print(String path) {
        try {
            StringBuilder out = new StringBuilder();
            out.append("\n");

            print(path, out, 0);
            return out.toString();
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return "";
        }
    }

    private void print(String path, StringBuilder out, int level) {
        for (String child : getChildren(path)) {
            for (int i = 0; i < level; i++) {
                out.append(" ");
            }
            out.append("└ ").append(child);
            out.append("\n");

            print(path + "/" + child, out, level + 1);
        }
    }

    private List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            log.debug("Cannot read children from path '{}', reason {}", path, e.getMessage(), e);
            return Collections.emptyList();
        } catch (Exception e) {
            log.warn("Cannot read children from path '{}', reason {}", path, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

}

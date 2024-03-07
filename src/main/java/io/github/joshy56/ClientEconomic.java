package io.github.joshy56;

import co.aikar.idb.BaseDatabase;
import co.aikar.idb.DB;
import co.aikar.idb.Database;
import co.aikar.idb.DatabaseOptions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * @author joshy56
 * @since 7/2/2024
 */
public class ClientEconomic extends JavaPlugin {
    Database database;

    @Override
    public void onEnable() {
        database = new BaseDatabase(DatabaseOptions.builder().sqlite("").build());

    }
}

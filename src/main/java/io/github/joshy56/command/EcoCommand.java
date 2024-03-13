package io.github.joshy56.command;

import com.google.common.base.Charsets;
import io.github.joshy56.Economic;
import io.github.joshy56.currency.Currency;
import io.github.joshy56.transaction.TransactionHandler;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.Sound;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabExecutor;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author joshy56
 * @since 11/3/2024
 */
public class EcoCommand implements TabExecutor {
    private final JavaPlugin plugin;
    private final Economic economic;

    public EcoCommand(JavaPlugin plugin, Economic economic) {
        this.plugin = plugin;
        this.economic = economic;
    }

    /**
     * @param sender
     * @param command
     * @param label
     * @param args
     * @return
     */
    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if(!command.isRegistered() || !(command.getLabel().equals("eco") || command.getLabel().equals("economic"))) return false;
        if(args.length == 0) {
            sender.sendMessage("Economic v:0.0.0.x-SNAPSHOT");
            return true;
        }

        String subCommandLabel = args[0];
        switch (subCommandLabel) {
            case "money" -> {
               break;
            }
            case "send" -> {
                if(args.length < 4) {
                    sender.sendMessage("Wrong use of command, correct format: /eco send <playerName> <currencyName> <amount>");
                    return true;
                }
                if(!(sender instanceof Player player)) {
                    sender.sendMessage("Sorry only players can send money to other player.");
                    return true;
                }
                if(!player.hasPermission("economic.send")) {
                    player.playSound(player.getEyeLocation(), Sound.ENTITY_VILLAGER_NO, 0.4f, 100);
                    player.sendMessage("Can't send money, u don't has permission.");
                    return true;
                }
                OfflinePlayer other = Bukkit.getOfflinePlayer(UUID.nameUUIDFromBytes("OfflinePlayer:".concat(args[1]).getBytes(Charsets.UTF_8)));
                if (!other.hasPlayedBefore()) {
                    sender.sendMessage("Player '" + args[1] + "' not has been played before.");
                    return true;
                }
                try {
                    double amount = Double.parseDouble(args[3]);
                    if(amount <= 0) {
                        player.sendMessage("Amount to send expect to be greater than zero.");
                        return true;
                    }
                    TransactionHandler handler = economic.transactionHandler().getOrThrow();
                    if(!handler.enoughMoney(player.getUniqueId(), args[2], amount).getOrThrow()) {
                        player.sendMessage("You don't has that amount of money.");
                        return true;
                    }
                    handler.withdraw(player.getUniqueId(), args[2], amount);
                    handler.deposit(other.getUniqueId(), args[2], amount);
                    if(other.isOnline()) {
                        other.getPlayer().sendMessage("Hey! the player " + player.getName() + " sends to you $" + amount);
                    }
                    return true;
                } catch (NumberFormatException ok) {
                    player.sendMessage("Amount to send is not a number.");
                    return true;
                } catch (Throwable ok) {
                    throw new RuntimeException(ok);
                }
            }
            default -> sender.sendMessage("Unknown command try with help.");
        }
        return false;
    }

    /**
     * @param sender
     * @param command
     * @param label
     * @param args
     * @return
     */
    @Nullable
    @Override
    public List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        return null;
    }
}

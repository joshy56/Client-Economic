package io.github.joshy56.transaction;

import io.github.joshy56.Economic;
import io.github.joshy56.currency.Currency;
import io.github.joshy56.currency.CurrencyRepository;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;
import io.github.joshy56.subject.Subject;
import io.github.joshy56.subject.SubjectRepository;
import io.github.joshy56.transaction.Transaction;
import io.github.joshy56.transaction.TransactionHandler;
import io.github.joshy56.transaction.TransactionRepository;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author joshy56
 * @since 12/2/2024
 */
public class SimpleTransactionHandler implements TransactionHandler {
    private final Economic economic;
    private final JavaPlugin plugin;

    public SimpleTransactionHandler(Economic economic, JavaPlugin plugin) {
        this.economic = economic;
        this.plugin = plugin;
    }

    @Override
    public Response<Double> balance(UUID subjectId, String currencyName) {
        try {
            TransactionRepository repository = economic.transactions().getOrThrow();
            Transaction transaction = repository.get(subjectId, currencyName).getOrThrow();
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transaction.amount()));
        } catch (Throwable ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException(String.format("Can't get balance of user with id: '%s' on currency with name: %s", subjectId.toString(), currencyName), ok)), Optional.empty());
        }
    }

    @Override
    public Response<Boolean> withdraw(UUID subjectId, String currencyName, double amount) {
        if (amount < 0)
            return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalArgumentException("Can't withdraw negative amount")), Optional.of(false));
        if (amount == 0) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(true));

        try {
            TransactionRepository repository = economic.transactions().getOrThrow();
            Transaction lastTransaction = repository.get(subjectId, currencyName).getOrThrow();
            Transaction newTransaction = new Transaction(currencyName, subjectId, (lastTransaction.amount() - amount));
            boolean opValue = repository.set(newTransaction).getOrThrow();
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(opValue));
        } catch (Throwable ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException(String.format("Can't withdraw $%f to user with id: '%s' on currency with name: %s", amount, subjectId, currencyName), ok)), Optional.of(false));
        }
    }

    @Override
    public Response<Boolean> deposit(UUID subjectId, String currencyName, double amount) {
        if (amount < 0)
            return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalArgumentException("Can't deposit negative amount")), Optional.of(false));
        if (amount == 0) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(true));

        try {
            TransactionRepository repository = economic.transactions().getOrThrow();
            Transaction lastTransaction = repository.get(subjectId, currencyName).getOrThrow();
            Transaction newTransaction = new Transaction(currencyName, subjectId, (lastTransaction.amount() + amount));
            boolean opValue = repository.set(newTransaction).getOrThrow();
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(opValue));
        } catch (Throwable ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException(String.format("Can't deposit $%f to user with id: '%s' on currency with name: %s", amount, subjectId, currencyName), ok)), Optional.of(false));
        }
    }

    @Override
    public Response<Boolean> enoughMoney(UUID subjectId, String currencyName, double amount) {
        try {
            double balance = balance(subjectId, currencyName).getOrThrow();
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(((balance - amount) >= 0)));
        } catch (Throwable ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException(String.format("Can't test if user with id: '%s' has amount $%f on currency with name: %s", subjectId, amount, currencyName))), Optional.of(false));
        }
    }

    @Override
    public Response<Set<Currency>> currenciesOf(UUID subjectId) {
        try {
            TransactionRepository transactionRepository = economic.transactions().getOrThrow();
            CurrencyRepository currencyRepository = economic.currencies().getOrThrow();
            Set<String> currenciesNames = transactionRepository.getAllOfSubject(subjectId).getOrThrow().parallelStream().map(Transaction::currencyName).collect(Collectors.toSet());
            Set<Currency> currencies = currencyRepository.getThey(currenciesNames).getOrThrow();
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(currencies));
        } catch (Throwable ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException(String.format("Can't get currencies of user with id: '%s'", subjectId.toString()), ok)), Optional.empty());
        }
    }

    @Override
    public Response<Set<Subject>> subjectsOf(String currencyName) {
        try {
            TransactionRepository transactionRepository = economic.transactions().getOrThrow();
            SubjectRepository subjectRepository = economic.subjects().getOrThrow();
            Set<UUID> subjectsIds = transactionRepository.getAllOfCurrency(currencyName).getOrThrow().parallelStream().map(Transaction::subjectIdentifier).collect(Collectors.toSet());
            Set<Subject> subjects = subjectRepository.getThey(subjectsIds).getOrThrow();
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(subjects));
        } catch (Throwable ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException(String.format("Can't get subjects of currency with name: '%s'", currencyName), ok)), Optional.empty());
        }
    }
}

package io.github.joshy56.currency;

import io.github.joshy56.response.Response;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

import static io.github.joshy56.response.ResponseCode.OK;

/**
 * @author joshy56
 * @since 10/3/2024
 */
public class SimpleCurrency implements Currency {
    private final String name;
    private String displayName, pluralName, abbreviation;
    private char symbol;

    public SimpleCurrency(@NotNull String name) {
        if(name.isBlank()) throw new IllegalArgumentException("SimpleCurrency@constructor() | Name can't be empty.");
        this.name = name;
    }

    /**
     * @return
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * @return
     */
    @Override
    public Response<String> displayName() {
        return new Response<>(OK, Optional.empty(), Optional.ofNullable(displayName));
    }

    /**
     * @param displayName
     * @return
     */
    @Override
    public Response<String> displayName(String displayName) {
        Response<String> response = new Response<>(OK, Optional.empty(), Optional.ofNullable(this.displayName));
        this.displayName = displayName.isBlank() ? null : displayName;
        return response;
    }

    /**
     * @return
     */
    @Override
    public Response<String> displayNamePlural() {
        return new Response<>(OK, Optional.empty(), Optional.ofNullable(pluralName));
    }

    /**
     * @param pluralName
     * @return
     */
    @Override
    public Response<String> displayNamePlural(String pluralName) {
        Response<String> response = new Response<>(OK, Optional.empty(), Optional.ofNullable(this.pluralName));
        this.pluralName = pluralName.isBlank() ? null : pluralName;
        return response;
    }

    /**
     * @return
     */
    @Override
    public Response<String> abbreviation() {
        return new Response<>(OK, Optional.empty(), Optional.ofNullable(abbreviation));
    }

    /**
     * @param abbreviation
     * @return
     */
    @Override
    public Response<String> abbreviation(String abbreviation) {
        Response<String> response = new Response<>(OK, Optional.empty(), Optional.ofNullable(this.abbreviation));
        this.abbreviation = abbreviation.isBlank() ? null : abbreviation;
        return response;
    }

    /**
     * @return
     */
    @Override
    public Response<Character> symbol() {
        return new Response<>(OK, Optional.empty(), Optional.of(symbol));
    }

    /**
     * @param symbol
     * @return
     */
    @Override
    public Response<Character> symbol(char symbol) {
        Response<Character> response = new Response<>(OK, Optional.empty(), Optional.of(this.symbol));
        this.symbol = symbol;
        return response;
    }
}

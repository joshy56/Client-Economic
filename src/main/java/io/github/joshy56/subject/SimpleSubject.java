package io.github.joshy56.subject;

import com.google.common.base.Strings;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;

import java.util.Optional;
import java.util.UUID;

/**
 * @author joshy56
 * @since 3/3/2024
 */
public class SimpleSubject implements Subject {
    private final UUID identifier;
    private String nickname;

    public SimpleSubject(UUID subjectId) {
        this.identifier = subjectId;
    }

    @Override
    public UUID identifer() {
        return this.identifier;
    }

    @Override
    public Response<String> nickname() {
        if(Strings.isNullOrEmpty(nickname)) return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalStateException("This subject don't has a nickname.")), Optional.empty());
        return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(nickname));
    }

    @Override
    public Response<String> nickname(String nickname) {
        String latestNickname = this.nickname;
        this.nickname = Strings.emptyToNull(nickname);
        return new Response<>(ResponseCode.OK, Optional.empty(), Optional.ofNullable(latestNickname));
    }
}

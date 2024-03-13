package io.github.joshy56.subject;

import co.aikar.idb.Database;
import co.aikar.idb.DbRow;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.github.joshy56.AbstractCachedRepository;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author joshy56
 * @since 3/3/2024
 */
public class SimpleSubjectRepository extends AbstractCachedRepository<UUID, Subject> implements SubjectRepository {
    public SimpleSubjectRepository(Database database) {
        super(database, CacheBuilder.newBuilder().expireAfterAccess(3, TimeUnit.MINUTES).expireAfterWrite(1, TimeUnit.MINUTES).build(
                new CacheLoader<>() {
                    @Override
                    public Subject load(UUID subjectId) throws Exception {
                        DbRow dbRow = database.getFirstRow("SELECT subjectId, nickname FROM subjects WHERE subjectId=?;", subjectId);
                        return Optional.ofNullable(dbRow)
                                .map(row -> {
                                    Subject subject = new SimpleSubject(subjectId);
                                    subject.nickname(row.getString("nickname"));
                                    return subject;
                                }).orElseThrow(() -> new NullPointerException("Subject don't exists."));
                    }
                }
        ));

        query(statement -> {
            try {
                statement.query("CREATE TABLE IF NOT EXISTS subjects(subjectId VARCHAR(36) PRIMARY KEY NOT NULL, nickname VARCHAR(64));");
                statement.executeUpdate();

                statement.commit();

                return Response.EMPTY();
            } catch (SQLException ok) {
                return Response.ERROR(ok);
            }
        });
    }

    /**
     * @param subjectId
     * @return
     */
    @Override
    public @NotNull Response<Subject> get(@NotNull UUID subjectId) {
        try {
            Subject subject = cache().get(subjectId);
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(subject));
        } catch (ExecutionException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, ups.", ok)), Optional.empty());
        }
    }

    /**
     * @param subjectsIds
     * @return
     */
    @Override
    public @NotNull Response<Set<Subject>> getAllOfThem(@NotNull Set<UUID> subjectsIds) {
        if (subjectsIds.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (UUID subjectId : subjectsIds) {
                    statement.query("SELECT subjectId, nickname FROM subjects WHERE subjectId=?;");
                    statement.execute(subjectId);
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                Set<Subject> subjects = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    Subject subject = new SimpleSubject(subjectId);
                    subject.nickname(row.getString("nickname"));
                    return subject;
                }).collect(Collectors.toSet());
                if (subjects.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(subjects));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Uh oh, something got wrong.", ok)), Optional.empty());
            }
        });
    }

    /**
     * @return
     */
    @Override
    public @NotNull Response<Set<Subject>> getAll() {
        return query(statement -> {
            try {
                statement.query("SELECT * FROM subjects;");
                statement.execute();

                statement.commit();

                Set<Subject> subjects = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    Subject subject = new SimpleSubject(subjectId);
                    subject.nickname(row.getString("nickname"));
                    return subject;
                }).collect(Collectors.toSet());
                if (subjects.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(subjects));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, ups.", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param subject
     * @return
     */
    @Override
    public @NotNull Response<Void> set(@NotNull Subject subject) {
        return query(statement -> {
            try {
                statement.query("INSERT INTO subjects(subjectId, nickname) VALUES(?, ?) ON CONFLICT(subjectId) DO UPDATE SET nickname=?;");
                statement.executeUpdate(subject.identifer(), subject.nickname(), subject.nickname());

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Oh...", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param subjects
     * @return
     */
    @Override
    public @NotNull Response<Void> setAll(@NotNull Set<Subject> subjects) {
        if (subjects.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (Subject subject : subjects) {
                    statement.query("INSERT INTO subjects(subjectId, nickname) VALUES(?, ?) ON CONFLICT(subjectId) DO UPDATE SET nickname=?;");
                    statement.executeUpdate(subject.identifer(), subject.nickname(), subject.nickname());
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("That it's", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param subjectId
     * @return
     */
    @Override
    public @NotNull Response<Void> delete(@NotNull UUID subjectId) {
        return query(statement -> {
            try {
                statement.query("DELETE FROM subjects WHERE subjectId=?;");
                statement.executeUpdate(subjectId);

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("I can't delete it...", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param subjectsIds
     * @return
     */
    @Override
    public @NotNull Response<Void> deleteAllOfThem(@NotNull Set<UUID> subjectsIds) {
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (UUID subjectId : subjectsIds) {
                    statement.query("DELETE FROM subjects WHERE subjectId=?;");
                    statement.executeUpdate(subjectId);
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("They're so power, can't delete then :p", ok)), Optional.empty());
            }
        });
    }

    /**
     * @return
     */
    @Override
    public @NotNull Response<Void> deleteAll() {
        return query(statement -> {
            try {
                statement.query("DELETE FROM subjects;");
                statement.executeUpdate();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("They're so much, can't delete.", ok)), Optional.empty());
            }
        });
    }
}

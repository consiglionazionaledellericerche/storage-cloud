/*
 * Copyright (C) 2019  Consiglio Nazionale delle Ricerche
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as
 *     published by the Free Software Foundation, either version 3 of the
 *     License, or (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package it.cnr.si.spring.storage;

/**
 * Created by mspasiano on 6/24/17.
 */
public class StorageException extends RuntimeException {
    private final Type type;

    public StorageException(Type type, Throwable throwable) {
        super(throwable);
        this.type = type;
    }

    public StorageException(Type type, String message, Throwable throwable) {
        super(message, throwable);
        this.type = type;
    }

    public StorageException(Type type, String message) {
        super(message);
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        CONSTRAINT_VIOLATED, CONTENT_ALREDY_EXISTS, INVALID_ARGUMENTS, UNAUTHORIZED, NOT_FOUND, GENERIC
    }
}

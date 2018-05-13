package com.jstructure.transactq.lib;

import java.sql.Connection;

public interface IConnectionManager {
    Connection createConnection();
}

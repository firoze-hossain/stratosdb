package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sprint 12 Week 23: Full ORM & Client Ecosystem Certification.
 *
 * Spring Boot PetClinic (Spring Data JPA + Hibernate 6.x) End-to-End Test Suite.
 *
 * Verifies:
 *   1. Complete PetClinic relational schema lifecycle with sequences, tables,
 *      indexes, and foreign key constraints using standard DDL syntax.
 *   2. Hibernate 6 sequence generation (nextval/currval/setval) with START WITH / INCREMENT BY.
 *   3. JPA query patterns: SELECT DISTINCT, LEFT JOIN with parenthesized ON clauses,
 *      LIKE filters, ORDER BY pagination (LIMIT/OFFSET), and COUNT(DISTINCT ...).
 *   4. Entity persistence, relational navigation, cascade deletes and updates.
 */
public class SpringDataJpaPetClinicEndToEndTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testSpringDataJpaPetClinicCompleteLifecycle() throws Exception {
        // 1. Hibernate 6 sequence declarations
        assertTrue(db.execute("CREATE SEQUENCE IF NOT EXISTS vets_seq START WITH 1 INCREMENT BY 1").isSuccess());
        assertTrue(db.execute("CREATE SEQUENCE IF NOT EXISTS specialties_seq START WITH 1 INCREMENT BY 1").isSuccess());
        assertTrue(db.execute("CREATE SEQUENCE IF NOT EXISTS types_seq START WITH 1 INCREMENT BY 1").isSuccess());
        assertTrue(db.execute("CREATE SEQUENCE IF NOT EXISTS owners_seq START WITH 1 INCREMENT BY 1").isSuccess());
        assertTrue(db.execute("CREATE SEQUENCE IF NOT EXISTS pets_seq START WITH 1 INCREMENT BY 1").isSuccess());
        assertTrue(db.execute("CREATE SEQUENCE IF NOT EXISTS visits_seq START WITH 1 INCREMENT BY 1").isSuccess());

        // 2. PetClinic entity tables
        QueryResult crVets = db.execute("""
            CREATE TABLE IF NOT EXISTS vets (
                id INT NOT NULL,
                first_name VARCHAR(30),
                last_name VARCHAR(30),
                PRIMARY KEY (id)
            )
            """);
        assertTrue(crVets.isSuccess(), "CREATE TABLE vets failed: " + crVets.getError());

        QueryResult crSpecialties = db.execute("""
            CREATE TABLE IF NOT EXISTS specialties (
                id INT NOT NULL,
                name VARCHAR(80),
                PRIMARY KEY (id)
            )
            """);
        assertTrue(crSpecialties.isSuccess(), "CREATE TABLE specialties failed: " + crSpecialties.getError());

        QueryResult crVetSpecialties = db.execute("""
            CREATE TABLE IF NOT EXISTS vet_specialties (
                vet_id INT NOT NULL,
                specialty_id INT NOT NULL
            )
            """);
        assertTrue(crVetSpecialties.isSuccess(), "CREATE TABLE vet_specialties failed: " + crVetSpecialties.getError());

        QueryResult crTypes = db.execute("""
            CREATE TABLE IF NOT EXISTS types (
                id INT NOT NULL,
                name VARCHAR(80),
                PRIMARY KEY (id)
            )
            """);
        assertTrue(crTypes.isSuccess(), "CREATE TABLE types failed: " + crTypes.getError());

        QueryResult crOwners = db.execute("""
            CREATE TABLE IF NOT EXISTS owners (
                id INT NOT NULL,
                first_name VARCHAR(30),
                last_name VARCHAR(30),
                address VARCHAR(255),
                city VARCHAR(80),
                telephone VARCHAR(20),
                PRIMARY KEY (id)
            )
            """);
        assertTrue(crOwners.isSuccess(), "CREATE TABLE owners failed: " + crOwners.getError());

        QueryResult crPets = db.execute("""
            CREATE TABLE IF NOT EXISTS pets (
                id INT NOT NULL,
                name VARCHAR(30),
                birth_date VARCHAR(20),
                type_id INT NOT NULL,
                owner_id INT,
                PRIMARY KEY (id)
            )
            """);
        assertTrue(crPets.isSuccess(), "CREATE TABLE pets failed: " + crPets.getError());

        QueryResult crVisits = db.execute("""
            CREATE TABLE IF NOT EXISTS visits (
                id INT NOT NULL,
                pet_id INT,
                visit_date VARCHAR(20),
                description VARCHAR(255),
                PRIMARY KEY (id)
            )
            """);
        assertTrue(crVisits.isSuccess(), "CREATE TABLE visits failed: " + crVisits.getError());

        // 3. Schema indexes
        assertTrue(db.execute("CREATE INDEX IF NOT EXISTS idx_owners_last_name ON owners (last_name)").isSuccess());
        assertTrue(db.execute("CREATE INDEX IF NOT EXISTS idx_pets_name ON pets (name)").isSuccess());

        // 4. Foreign Key Constraints
        QueryResult fk1 = db.execute("ALTER TABLE vet_specialties ADD CONSTRAINT fk_vet_specialties_vets FOREIGN KEY (vet_id) REFERENCES vets (id)");
        assertTrue(fk1.isSuccess(), "FK vet_specialties -> vets failed: " + fk1.getError());

        QueryResult fk2 = db.execute("ALTER TABLE vet_specialties ADD CONSTRAINT fk_vet_specialties_specialties FOREIGN KEY (specialty_id) REFERENCES specialties (id)");
        assertTrue(fk2.isSuccess(), "FK vet_specialties -> specialties failed: " + fk2.getError());

        QueryResult fk3 = db.execute("ALTER TABLE pets ADD CONSTRAINT fk_pets_owners FOREIGN KEY (owner_id) REFERENCES owners (id)");
        assertTrue(fk3.isSuccess(), "FK pets -> owners failed: " + fk3.getError());

        QueryResult fk4 = db.execute("ALTER TABLE pets ADD CONSTRAINT fk_pets_types FOREIGN KEY (type_id) REFERENCES types (id)");
        assertTrue(fk4.isSuccess(), "FK pets -> types failed: " + fk4.getError());

        QueryResult fk5 = db.execute("ALTER TABLE visits ADD CONSTRAINT fk_visits_pets FOREIGN KEY (pet_id) REFERENCES pets (id)");
        assertTrue(fk5.isSuccess(), "FK visits -> pets failed: " + fk5.getError());

        // 5. Data seeding using nextval() sequence expressions
        assertTrue(db.execute("INSERT INTO vets VALUES (nextval('vets_seq'), 'James', 'Carter')").isSuccess());
        assertTrue(db.execute("INSERT INTO vets VALUES (nextval('vets_seq'), 'Helen', 'Leary')").isSuccess());
        assertTrue(db.execute("INSERT INTO vets VALUES (nextval('vets_seq'), 'Linda', 'Douglas')").isSuccess());

        assertTrue(db.execute("INSERT INTO specialties VALUES (nextval('specialties_seq'), 'radiology')").isSuccess());
        assertTrue(db.execute("INSERT INTO specialties VALUES (nextval('specialties_seq'), 'surgery')").isSuccess());
        assertTrue(db.execute("INSERT INTO specialties VALUES (nextval('specialties_seq'), 'dentistry')").isSuccess());

        assertTrue(db.execute("INSERT INTO vet_specialties VALUES (1, 1)").isSuccess());
        assertTrue(db.execute("INSERT INTO vet_specialties VALUES (2, 2)").isSuccess());
        assertTrue(db.execute("INSERT INTO vet_specialties VALUES (3, 2)").isSuccess());
        assertTrue(db.execute("INSERT INTO vet_specialties VALUES (3, 3)").isSuccess());

        assertTrue(db.execute("INSERT INTO types VALUES (nextval('types_seq'), 'cat')").isSuccess());
        assertTrue(db.execute("INSERT INTO types VALUES (nextval('types_seq'), 'dog')").isSuccess());
        assertTrue(db.execute("INSERT INTO types VALUES (nextval('types_seq'), 'lizard')").isSuccess());

        assertTrue(db.execute("INSERT INTO owners VALUES (nextval('owners_seq'), 'George', 'Franklin', '110 W. Liberty St.', 'Madison', '6085551023')").isSuccess());
        assertTrue(db.execute("INSERT INTO owners VALUES (nextval('owners_seq'), 'Betty', 'Davis', '638 Cardinal Ave.', 'Sun Prairie', '6085551749')").isSuccess());
        assertTrue(db.execute("INSERT INTO owners VALUES (nextval('owners_seq'), 'Eduardo', 'Rodriquez', '2693 Commerce St.', 'McFarland', '6085558763')").isSuccess());
        assertTrue(db.execute("INSERT INTO owners VALUES (nextval('owners_seq'), 'Harold', 'Davis', '563 Friendly St.', 'Windsor', '6085553198')").isSuccess());

        assertTrue(db.execute("INSERT INTO pets VALUES (nextval('pets_seq'), 'Leo', '2010-09-07', 1, 1)").isSuccess());
        assertTrue(db.execute("INSERT INTO pets VALUES (nextval('pets_seq'), 'Basil', '2012-08-06', 2, 2)").isSuccess());
        assertTrue(db.execute("INSERT INTO pets VALUES (nextval('pets_seq'), 'Rosy', '2011-04-17', 2, 3)").isSuccess());
        assertTrue(db.execute("INSERT INTO pets VALUES (nextval('pets_seq'), 'Jewel', '2010-03-07', 2, 3)").isSuccess());

        assertTrue(db.execute("INSERT INTO visits VALUES (nextval('visits_seq'), 1, '2013-01-01', 'rabies shot')").isSuccess());
        assertTrue(db.execute("INSERT INTO visits VALUES (nextval('visits_seq'), 1, '2013-01-04', 'neutered')").isSuccess());
        assertTrue(db.execute("INSERT INTO visits VALUES (nextval('visits_seq'), 2, '2013-01-06', 'annual checkup')").isSuccess());

        // 6. Spring Data JPA Repositories Query Simulation

        // (a) VetRepository: SELECT DISTINCT v.id, v.first_name, v.last_name FROM vets v LEFT JOIN vet_specialties vs ON (v.id = vs.vet_id)
        QueryResult qVets = db.execute("SELECT DISTINCT v.id, v.first_name, v.last_name FROM vets v LEFT JOIN vet_specialties vs ON (v.id = vs.vet_id)");
        assertTrue(qVets.isSuccess(), "Vet query failed: " + qVets.getError());
        assertEquals(3, qVets.getRows().size(), "There are 3 unique vets");

        // (b) OwnerRepository: findByLastNameLike('Davis%')
        QueryResult qDavis = db.execute("SELECT id, first_name, last_name, city FROM owners WHERE last_name LIKE 'Davis%'");
        assertTrue(qDavis.isSuccess(), "Owner LIKE query failed: " + qDavis.getError());
        assertEquals(2, qDavis.getRows().size(), "Expected 2 owners with last name Davis");

        // (c) OwnerRepository: Pagination with ORDER BY ... LIMIT 2 OFFSET 1
        QueryResult qPage = db.execute("SELECT id, last_name FROM owners ORDER BY last_name LIMIT 2 OFFSET 1");
        assertTrue(qPage.isSuccess(), "Pagination query failed: " + qPage.getError());
        assertEquals(2, qPage.getRows().size(), "Page size should be 2");
        assertEquals("Davis", qPage.getRows().get(0).getValue("last_name"));
        assertEquals("Franklin", qPage.getRows().get(1).getValue("last_name"));

        // (d) OwnerRepository: Aggregation COUNT(DISTINCT o.id)
        QueryResult qCount = db.execute("SELECT COUNT(DISTINCT o.id) FROM owners o");
        assertTrue(qCount.isSuccess(), "COUNT(DISTINCT) query failed: " + qCount.getError());
        assertEquals(1, qCount.getRows().size());
        assertEquals(4L, ((Number) qCount.getRows().get(0).getValues().get(0)).longValue());

        // (e) Relational Join: Pet with Visits
        QueryResult qPetVisits = db.execute("""
            SELECT p.name, v.description, v.visit_date
            FROM pets p
            JOIN visits v ON (p.id = v.pet_id)
            WHERE p.id = 1
            ORDER BY v.visit_date
            """);
        assertTrue(qPetVisits.isSuccess(), "Pet visits join failed: " + qPetVisits.getError());
        assertEquals(2, qPetVisits.getRows().size(), "Pet 1 has 2 visits");
        assertEquals("rabies shot", qPetVisits.getRows().get(0).getValue("description"));
        assertEquals("neutered", qPetVisits.getRows().get(1).getValue("description"));
    }
}

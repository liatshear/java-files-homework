package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Submission;
import smarticulous.db.User;
import smarticulous.db.Exercise.Question;

import java.sql.*;

import java.util.ArrayList;
import java.util.List;

import javax.naming.spi.DirStateFactory.Result;

/**
 * The Smarticulous class, implementing a grading system.
 * @param <SQLiteDatabase>
 * @param <Cursor>
 */
public class Smarticulous<SQLiteDatabase, Cursor> {
    //Data base version
    //private static final int DATABASE_VERSION = 1;
    //data base Name
    //private static final String DATABASE_NAME = "UserManager.db";




    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     *
     * <table>
     *   <caption><em>Table name: <strong>User</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>UserId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Username</td><td>Text</td></tr>
     *   <tr><td>Firstname</td><td>Text</td></tr>
     *   <tr><td>Lastname</td><td>Text</td></tr>
     *   <tr><td>Password</td><td>Text</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Exercise</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>DueDate</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Question</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>Desc</td><td>Text</td></tr>
     *   <tr><td>Points</td><td>Integer</td></tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise the primary key.
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Submission</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>UserId</td><td>Integer</td></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>SubmissionTime</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Grade</td><td>Real</td></tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */


    
    public Connection openDB(String dburl) throws SQLException {
        // create strings to create tables user, exericse, question, submission and questiongrade
        String Usertable = "CREATE TABLE IF NOT EXISTS User (UserId INTEGER PRIMARY KEY, Username TEXT UNIQUE, Firstname TEXT, Lastname TEXT, Password TEXT);";
        String ExerciseTable = "CREATE TABLE IF NOT EXISTS Exercise (ExerciseId INTEGER PRIMARY KEY, Name TEXT, DueDate INTEGER);";
        String QuestionTable = "CREATE TABLE IF NOT EXISTS Question (ExerciseId INTEGER, QuestionId INTEGER, Name TEXT, Desc TEXT, Points INTEGER, PRIMARY KEY (ExerciseId, QuestionId));";
        String SubmissionTable = "CREATE TABLE IF NOT EXISTS Submission (SubmissionId INTEGER PRIMARY KEY, UserId INTEGER, ExerciseId INTEGER, SubmissionTime INTEGER);";
        String QuestionGradeTable = "CREATE TABLE IF NOT EXISTS QuestionGrade (SubmissionId INTEGER, QuestionId INTEGER, Grade REAL, PRIMARY KEY (SubmissionId, QuestionId));";
        try{
            //get connection to dburl
            db = DriverManager.getConnection(dburl);
            Statement st = db.createStatement();
            //create table User, Exercise, Question, Submission,QuestionGrade
            st.execute(Usertable);
            st.execute(ExerciseTable);
            st.execute(QuestionTable);
            st.execute(SubmissionTable);
            st.execute(QuestionGradeTable);
            st.close();
        } catch (SQLException e){
            System.out.println("Error connecting to SQLite database");
        }
        // set auto commit to false and return connection
        db.setAutoCommit(false);
        return db;
    }

    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }


    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        Statement st = db.createStatement();
        String checkUser = "SELECT * FROM User WHERE Username='"+user.username+"';";
        String insertUser = "INSERT INTO User (Username, Firstname, Lastname, Password) " + "VALUES (?, ?, ?, ?);";
        String UpdateUser = "UPDATE User SET Firstname=?, Lastname=?, Password=? WHERE Username=?;";
        String getId = "SELECT UserId FROM User WHERE Username='"+user.username+"';";
        // prepare statements to insert and update users and add values to the queries 
        PreparedStatement ps = db.prepareStatement(insertUser);
        PreparedStatement pst = db.prepareStatement(UpdateUser);
        ps.setString(1, user.username);
        ps.setString(2, user.firstname);
        ps.setString(3, user.lastname);
        ps.setString(4, password);
        pst.setString(1, user.firstname);
        pst.setString(2, user.lastname);
        pst.setString(3, password);
        pst.setString(4, user.username);
        // check if user exists in the system or not
        ResultSet rs = st.executeQuery(checkUser);
        int userId = 0;
        ResultSet generatedKeys;
        // if the result set is empty, no user exists and thus INSERT user
        if(!rs.next()){
            ps.executeUpdate();
            // get the last generated userid using the prepared statement 
            generatedKeys = ps.getGeneratedKeys(); 
            if(generatedKeys.next()){
                // only one id will be returned since there was a single update
                userId = generatedKeys.getInt(1);
            }
        }
        // the user was found and therefore we need to update their information
        else{
            pst.executeUpdate();
            // get their user id using the prepared statement and return UserId
            generatedKeys = st.executeQuery(getId);
            while(generatedKeys.next()){
                userId = generatedKeys.getInt("UserId");
            }
        }
        // close all statements and resultsets that were open, as well as commit and return the user id
        generatedKeys.close();
        ps.close();
        pst.close();
        db.commit();
        return userId;
    }


    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * @throws SQLException
     * <p>
     * Note: this is totally insecure. For real-life password checking, it's important to store only
     * a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        Statement stmt = db.createStatement();
        String checkUser = "SELECT Password FROM User WHERE Username='"+ username +"';";
        //check if there is a row in the table User with the same username
        ResultSet rs = stmt.executeQuery(checkUser);
        db.commit();
        // check in the result set for the password of the user if they were found in the system
        while(rs.next()){
            // compare the password of the given username in the database to the password given by user
            if(rs.getString("Password").equals(password)){
            return true;
            }
        }
        // the passwords didnt match or the user was not found in the system
        return false;
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        Statement st = db.createStatement();
        String checkUser = "SELECT * FROM Exercise WHERE ExerciseId='"+exercise.id+"';";
        String insertInto = "INSERT INTO Exercise (ExerciseId, Name, DueDate) VALUES ("+exercise.id+",'"+exercise.name+"',"+exercise.dueDate.getTime()+");";
        PreparedStatement ps = db.prepareStatement(insertInto);
        ResultSet rs = st.executeQuery(checkUser);
        // check if a user with the id already exists in the database
        if(!rs.next()){
            //if the set is empty, no exercise with this id exists in the database and therefore insert the new exercise 
            ps.executeUpdate(); 
            st.close();
            // now go through each question in the exercise
            for(Exercise.Question question: exercise.questions){
                st = db.createStatement();
                // insert each question from the exercise into Question
                String insertQuestion = "INSERT INTO Question (ExerciseId, Name, Desc, Points) VALUES('"+exercise.id+"', '"+question.name+"','"+question.desc+"',"+question.points+ ");";
                st.executeUpdate(insertQuestion);
                st.close();
            }
            db.commit();
            // use the prepared statement to get the user id
            ResultSet generatedKeys = ps.getGeneratedKeys();
            int exId = 0;
            if(generatedKeys.next()) {
                // this will return the userid as its the only generated key in the update
                exId = generatedKeys.getInt(1);
            }
            // close all statements which have been opened and return the generated userid
            ps.close();
            generatedKeys.close();
            return exId;
        }
        // therefore a user with this id already exists in the database and thus return -1 as required
        else{
            return -1;
        }
    }
      
    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     * @param <MyType>
     *
     * @return list of all exercises.
     * @throws SQLException
     */


    public <MyType> List<Exercise> loadExercises() throws SQLException {
        // create array list of exercises to return
        List<Exercise> returnList = new ArrayList<Exercise>();
        Statement st1 = db.createStatement();
        // select queries to get all the exercises from Exercise and to get the questions from the exercise
        String GetQuestions = "SELECT * FROM Question INNER JOIN Exercise ON " + "Exercise.ExerciseId = Question.ExerciseId ORDER BY Exercise.ExerciseId, QuestionId;";
        String GetExercises = "SELECT *, COUNT(QuestionId) FROM Exercise INNER JOIN Question ON " + "Exercise.ExerciseId = Question.ExerciseId GROUP BY Question.ExerciseId ORDER BY Exercise.ExerciseId;";
        // execute the queries to get the exercises and the questions
        ResultSet es = st1.executeQuery(GetExercises);
        Statement st2 = db.createStatement();
        ResultSet qs = st2.executeQuery(GetQuestions);
        // create while loop to iterate through all the exercises
        while(es.next()){
            int exID = es.getInt("ExerciseId");
            String name = es.getString("Name");
            // create new exercise using the current exercises exid, name and date
            Exercise current = new Exercise(exID, name, new Date(es.getInt("DueDate")));
            // assign numEx to be the number of questions in the exercise
            int numEx = es.getInt("COUNT(QuestionId)");
            int cur = 0;
            // create a for loop to iterate through the questions array in the exercise and add each one to the current exercise
            for(cur = 0; cur < numEx; cur++){
                qs.next();
                current.addQuestion(qs.getString("Name"), qs.getString("Desc"), qs.getInt("Points"));
            }
            returnList.add(current);
        }
        // close statements and return exercise list
        st1.close();
        st2.close();
        db.commit();
        return returnList;
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        Statement st = db.createStatement();
        // sql queries to check if user exists in the database
        String checkUser = "SELECT UserId FROM User WHERE Username='"+submission.user.username+"';"; 
        // sql query to get the submission id to return
        String GetSubId = "SELECT SubmissionId FROM Submission ORDER BY SubmissionId DESC LIMIT 1;";
        //check if the user exists in the data base
        ResultSet rs = st.executeQuery(checkUser);
        int UserID = 0;
        //if the result set is empty then the user doesnt exist in the database and thus return -1
        if(!rs.next()){
            return -1;
        }
        // else the user is in the database and we get their userId
        else{
            UserID = rs.getInt("UserId");
            rs.close();
            st.close();
            db.commit();
        }
        
        // we now know the user exists in the database and have their user id
        int subID = submission.id;
        // sql queries to insert with and without the submission id
        String InsertSubNoId = "INSERT INTO Submission (UserId, ExerciseId, SubmissionTime) VALUES("+UserID+","+submission.exercise.id+", "+submission.submissionTime.getTime()+");";
        String InsertSub = "INSERT INTO Submission (SubmissionId, UserId, ExerciseId, SubmissionTime) VALUES("+submission.id+","+ UserID +", "+submission.exercise.id+", "+submission.submissionTime.getTime()+");";
        if (subID == -1){
            //if the submission id is -1 then insert submission WITHOUT the submission id, ignore it
            Statement stmt2 = db.createStatement();
            stmt2.executeUpdate(InsertSubNoId);
            // get their submission id into a result set 
            ResultSet getsubId = stmt2.executeQuery(GetSubId);
            if(getsubId.next()){
                subID = getsubId.getInt("SubmissionId");
            }
            // close all statements and result sets that were opened and return the submission id that was retrieved
            stmt2.close();
            getsubId.close();
            db.commit();
            return subID;
        }
        else{ // this means the submission id is not -1 and therefore insert WITH the submissionid field
            Statement stmt1 = db.createStatement();
            stmt1.executeUpdate(InsertSub);
            stmt1.close();
            // commit the change and return the normal submission id since we have it and it != -1
            db.commit();      
            return subID;
        } 
    }


    // ============= Submission Query ===============


    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
        Statement st = db.createStatement();
        String query= "SELECT Question" +
               " FROM Exercise USING (ExerciseId)" +
               " Submission Using (SubmissionId)" +
               " User USING (Username)LIMIT 1" +
               "SORT BY QuestionID ASC" +
               "GROUP BY SubmissionID, QuestionId, Grade, SubmissionTime";
        st.close();
        return null;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    // didnt do bonus
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}

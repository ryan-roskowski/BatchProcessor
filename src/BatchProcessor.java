
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchProcessor {

	public static void main(String[] args) throws IOException, InterruptedException {
		WatchService watchService = FileSystems.getDefault().newWatchService();
		Path path = Paths.get("AccountInputFiles");
		path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
		WatchKey key;

		ExecutorService es = Executors.newFixedThreadPool(30); // 30 files at a time max
		System.out.println("Listening...");
		while ((key = watchService.take()) != null) {
			for (WatchEvent<?> event : key.pollEvents()) {
				System.out.println("Event occured: File " + event.context() + " added.");
				System.out.println("Creating new Thread to process...");
				es.execute(new Thread() {
					public void run() {
						try {
						
							Thread.sleep(100); // Occasionally windows explorer still has the file "in use" from copying,
												// give 100ms time.
	
							// Set up for parsing the xml file
							File xmlFile = new File("AccountInputFiles/" + event.context().toString());
							System.out.println(xmlFile.getAbsolutePath());
							DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
							DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
							Document doc = dBuilder.parse(xmlFile);
							doc.getDocumentElement().normalize();
	
							// Set up for database connection
							Class.forName("oracle.jdbc.driver.OracleDriver");
							Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "system",
									"password");
	
							NodeList nList = doc.getElementsByTagName("Account");
							for (int i = 0; i < nList.getLength(); i++) {
								Node nNode = nList.item(i);
								if (nNode.getNodeType() == Node.ELEMENT_NODE) {
									Element eElement = (Element) nNode;
									String accountId = eElement.getElementsByTagName("AccountID").item(0).getTextContent();
									String accountNum = eElement.getElementsByTagName("AccNum").item(0).getTextContent();
									String accType = eElement.getElementsByTagName("AccountType").item(0).getTextContent();
									String bankName = eElement.getElementsByTagName("BankName").item(0).getTextContent();
									Node custInfo = eElement.getElementsByTagName("CustomerDetails").item(0);
									Element custElement = (Element) custInfo;
									String firstName = custElement.getElementsByTagName("FirstName").item(0)
											.getTextContent();
									String lastName = custElement.getElementsByTagName("LastName").item(0).getTextContent();
									String address = custElement.getElementsByTagName("Address").item(0).getTextContent();
									String phone = custElement.getElementsByTagName("Phone").item(0).getTextContent();
	
									PreparedStatement ps = conn.prepareStatement(
											"SELECT * FROM BANKING_APPLICATION_CUSTOMERS WHERE FIRST_NAME = ? AND LAST_NAME = ? AND ADDRESS = ? AND PHONE = ?");
									ps.setString(1, firstName);
									ps.setString(2, lastName);
									ps.setString(3, address);
									ps.setString(4, phone);
									ResultSet rs = ps.executeQuery();
									if (rs.next()) {
										// Customer already exists, get ID to add the Account
										int custId = rs.getInt("ID");
	
										// Since this is multithreaded, need to get the next sequence value first for
										// later use. Using currval later may be incorrect since another thread can call
										// nextval at any time.
										Statement getAccountPrimaykey = conn.createStatement();
										ResultSet accountPrimayKey = getAccountPrimaykey.executeQuery(
												"Select BANKING_APPLICATION_ACCOUNTS_PKSEQ.nextval from dual");
										accountPrimayKey.next();
										int accPkNextval = accountPrimayKey.getInt(1);
	
										PreparedStatement insertAccount = conn.prepareStatement(
												"INSERT INTO BANKING_APPLICATION_ACCOUNTS (ID, ACCOUNT_NUMBER, TYPE, CUSTOMER_ID) VALUES (?, BANKING_APPLICATION_ACCOUNTS_ACCOUNT_NUMBER.nextval, ?, ?)");
										insertAccount.setInt(1, accPkNextval);
										insertAccount.setString(2, accType);
										insertAccount.setInt(3, custId);
										insertAccount.executeUpdate();
	
										PreparedStatement insertAccImport = conn.prepareStatement(
												"INSERT INTO BANKING_APPLICATION_ACCOUNT_IMPORTS (ID, PREVID, PREV_ACC_NUM, PREV_BANK, NEW_ACC_ID) VALUES (BANKING_APPLICATION_ACCOUNT_IMPORTS_PKSEQ.nextval, ?, ?, ?, ?)");
										insertAccImport.setInt(1, Integer.parseInt(accountId));
										insertAccImport.setInt(2, Integer.parseInt(accountNum));
										insertAccImport.setString(3, bankName);
										insertAccImport.setInt(4, accPkNextval);
										insertAccImport.executeUpdate();
										System.out.println("0 Customers added, 1 Account added.");
									} 
									else {
										// Need to add customer as well.
	
										// first, get customer pk nextval
										Statement getCustomerPrimayKey = conn.createStatement();
										ResultSet custPk = getCustomerPrimayKey.executeQuery(
												"Select BANKING_APPLICATION_CUSTOMERS_PKSEQ.nextval from dual");
										custPk.next();
										int custPknext = custPk.getInt(1);
	
										PreparedStatement addCustomer = conn.prepareStatement(
												"INSERT INTO BANKING_APPLICATION_CUSTOMERS (ID, FIRST_NAME, LAST_NAME, ADDRESS, PHONE) VALUES (?, ?, ?, ?, ?)");
										addCustomer.setInt(1, custPknext);
										addCustomer.setString(2, firstName);
										addCustomer.setString(3, lastName);
										addCustomer.setString(4, address);
										addCustomer.setString(5, phone);
										addCustomer.executeUpdate();
	
										// get account primary key next value
										Statement getAccPk = conn.createStatement();
										ResultSet Accpk = getAccPk.executeQuery(
												"Select BANKING_APPLICATION_ACCOUNTS_PKSEQ.nextval from dual");
										Accpk.next();
										int accPkNext = Accpk.getInt(1);
	
										PreparedStatement insertAcc = conn.prepareStatement(
												"INSERT INTO BANKING_APPLICATION_ACCOUNTS (ID, ACCOUNT_NUMBER, TYPE, CUSTOMER_ID) VALUES (?, BANKING_APPLICATION_ACCOUNTS_ACCOUNT_NUMBER.nextval, ?, ?)");
										insertAcc.setInt(1, accPkNext);
										insertAcc.setString(2, accType);
										insertAcc.setInt(3, custPknext);
										insertAcc.executeUpdate();
										System.out.println("1 Customer added, 1 Account added.");
	
										PreparedStatement insertImport = conn.prepareStatement(
												"INSERT INTO BANKING_APPLICATION_ACCOUNT_IMPORTS (ID, PREVID, PREV_ACC_NUM, PREV_BANK, NEW_ACC_ID) VALUES (BANKING_APPLICATION_ACCOUNT_IMPORTS_PKSEQ.nextval, ?, ?, ?, ?)");
										insertImport.setInt(1, Integer.parseInt(accountId));
										insertImport.setInt(2, Integer.parseInt(accountNum));
										insertImport.setString(3, bankName);
										insertImport.setInt(4, accPkNext);
										insertImport.executeUpdate();
									}
								}
							}
							conn.close();
							
							// move file to a different directory
							Files.move(Paths.get("AccountInputFiles/" + event.context().toString()),Paths.get("AccountOutputFiles/" + event.context().toString()),StandardCopyOption.REPLACE_EXISTING);
						} catch(Exception e) {
							e.printStackTrace();
						}

					}
				});

			}
			key.reset();
		}
		
	}

}

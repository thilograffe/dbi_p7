package jdbcPostgres;

import java.util.Scanner;

public class ControlClass {
	private static int threadCount = 0;
	private static int callbackCount = 0;
	private static long time = 0;
	
	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		System.out.println("Herzlich willkommen zum Control-Panel des Datenbank-Systems der Gruppe dbi32!\n"
							+ "M�chten sie die Datenbank reseten oder bef�llen? (Geben sie 0 f�r Ersteres oder 1 f�r letzteres ein!)");
		int mode = scan.nextInt();
		if(mode==1||mode==0) {
			if(mode==0) { //Modus zum L�schen und neu Erstellen der Tabellen
				new TaskClass().initialize();
			}
			else { //Modus zum F�llen der Tabellen
				new TaskClass().setTableTriggers(false); //Fremdschl�ssel�berpr�fung wird ausgeschaltet
				
				//Eingaben:
				System.out.println("Wie hoch soll der Skalierungsfaktor sein?");
				int n = scan.nextInt();
				System.out.println("Wie gro� soll die Batchgr��e sein?");
				int bs = scan.nextInt();
				System.out.println("Wie viele Threads?");
				threadCount = scan.nextInt();
				//Eingaben werden in die statischen variablen der TaskClass geschrieben
				TaskClass.configure(n,bs,threadCount);
				
				time=System.currentTimeMillis(); //Hier wird die Stoppuhr gestartet.
				
				//Treads werden erstellt
				for(int i=0;i<threadCount;i++) {
					new Thread(new TaskClass(i)).start();
				}
				
			}
		}
		else {
			System.out.println("Das ist weder eine 0 noch eine 1!");
		}
		scan.close();
	}
	
	public static void callback(int threadIndex) { //Callbacks werden von den einzelnen Threads gesendet, sobals sie fertig sind.
		callbackCount++;
		if(callbackCount==threadCount) {
			System.out.println("Benchmark-Ergebnis: "+(System.currentTimeMillis()-time)); //Hier wird die Stoppuhr gestoppt.
			new TaskClass().setTableTriggers(true);  //Fremdschl�ssel�berpr�fung wird eingeschaltet
		}
	}
}
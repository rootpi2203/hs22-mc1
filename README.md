# Mini-Challenge 1 - High Performance Computing (hpc) HS22

## Dokumentation und starten der Applikation
**Dokumentation und Struktur**  
1. Die Aufgabenstellung der hpc Mini Challenge kann aus dem `Task_description` File entnommen werden
2. Eine detaillierte Bearbeitung der Aufgaben sowie Anleitungen zur Anwendung können im File `report/hpc_ch1_manuel` nachgeschlagen werden.
3. Die Python file der einzelnen Container, sind direkt im `image` Ordner hinterlegt.
4. Der Ordner `notebooks` umfasst das Volume des default Jupyter Container


**Schnell Start der Anwendung**  
(detaillierte Anleitungen aus der Dokumentation entnehmen)
1. `docker-compose up --build` ausführen.
2. Nach dem Aufbau der Container kann im Docker Windows die Nachrichten der einzelnen Container geprüft werden.
3. Um den Nachrichtenfluss der Anwendung zu sehen, kann im Container `jupyter_perf` das Notebook `consum_performance` ausgeführt werden. (*Achtung: Portumleitung beachten port:8880:8888*)
# Initialize an empty dictionary
pdgId_dict = {}
import json
import particle

print(particle.Particle.from_pdgid(11).is_self_conjugate)


# Open and read the file
with open('data.txt', 'r') as file:
    for line in file:
        # Skip the header or any empty lines
        if line.strip() and not line.startswith('PDGID'):
            # Split the line by comma and strip whitespace
            parts = line.split(',')
            pdgId = int(parts[0].strip())
            evtGenName = parts[1].strip()
            try:
                isconjugate = particle.Particle.from_pdgid(pdgId).is_self_conjugate
            except Exception as e:
                # If an error occurs, use a default value
                print(f"Error for PDGID {pdgId}: {e}, setting isconjugate to False")
                isconjugate = "doodoo"
            # Add to the dictionary
            pdgId_dict[pdgId] = [evtGenName, isconjugate]

# Print the dictionary to verify
for key, value in pdgId_dict.items():
    print(f"{key}: {value}")
with open("pdgid_dict.json", "w") as file:
    json.dump(pdgId_dict, file, indent=4)

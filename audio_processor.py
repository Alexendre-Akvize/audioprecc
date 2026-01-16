import numpy as np
import librosa
from pydub import AudioSegment
from pydub.generators import WhiteNoise, Sine
import random
import os

def detect_bpm(file_path):
    try:
        y, sr = librosa.load(file_path, duration=120)
        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
        if hasattr(tempo, 'item'):
            bpm = round(tempo.item())
        elif isinstance(tempo, np.ndarray):
            bpm = round(float(tempo[0])) if tempo.size > 0 else 120
        else:
            bpm = round(tempo)
        
        # Validate BPM range (typical dance music: 60-200 BPM)
        if bpm < 60 or bpm > 200:
            print(f"Warning: BPM {bpm} out of range, defaulting to 120")
            return 120
            
        return bpm
    except Exception as e:
        print(f"Error detecting BPM: {e}")
        return 120

def find_drop_start(inst_segment, beat_ms, sr=44100):
    """
    Finds the start of the drop (loudest 32-beat section).
    Skips the first 15 seconds to avoid loud intros.
    """
    ms_32_beats = 32 * beat_ms
    
    # Convert to mono for analysis
    mono_inst = inst_segment.set_channels(1)
    
    # Skip first 15 seconds if track is long enough
    start_offset = 15000 if len(mono_inst) > 45000 else 0
    
    analysis_audio = mono_inst[start_offset:]
    
    if len(analysis_audio) < ms_32_beats:
        return 0
        
    # We will slide a window of 32 beats and find max RMS
    # To save time, we can sample every 1 beat (approx)
    step = int(beat_ms)
    max_rms = 0
    best_start = 0
    
    # Limit search to first 2 minutes + offset to save time? 
    # Or search whole track. Drops are usually in 0:45 - 2:00 range.
    search_duration = min(len(analysis_audio), 4 * 60 * 1000) # Search up to 4 mins
    
    # Iterate
    # Pydub RMS calculation can be slow on long segments in a loop.
    # Faster approach: use array of samples.
    samples = np.array(analysis_audio.get_array_of_samples())
    if analysis_audio.sample_width == 2:
        samples = samples.astype(np.int16)
    elif analysis_audio.sample_width == 4:
        samples = samples.astype(np.int32)
    
    # samples is 1D array (mono)
    # Calculate window size in samples
    # beat_ms is ms. sample_rate is samples/sec -> samples/ms = sr/1000
    samples_per_ms = analysis_audio.frame_rate / 1000
    window_samples = int(ms_32_beats * samples_per_ms)
    step_samples = int(step * samples_per_ms)
    
    # Square the samples for RMS
    # Use float to avoid overflow
    sq_samples = samples.astype(np.float64) ** 2
    
    # Use convolution for sliding window sum of squares (faster than loop)
    # But for very large arrays, convolution can be heavy.
    # Let's use a simplified stride loop
    
    num_steps = (len(samples) - window_samples) // step_samples
    
    best_idx = 0
    max_energy = -1
    
    for i in range(0, len(samples) - window_samples, step_samples):
        # Calculate sum of squares in this window
        # To optimize, we could use integral image / prefix sum, but python loop with numpy slice is okay for audio < 5min
        # Actually, numpy slice is fast.
        current_energy = np.sum(sq_samples[i : i+window_samples])
        if current_energy > max_energy:
            max_energy = current_energy
            best_idx = i
            
    # Convert best_idx back to ms
    best_start_ms = int(best_idx / samples_per_ms) + start_offset
    
    # Align to nearest beat? 
    # Ideally yes, but "exact drop" might not be exactly on our beat grid if bpm varies slightly.
    # We will return the found time.
    
    return best_start_ms

def generate_clap(duration_ms=200):
    # Check if we have a custom clap sample
    sample_path = os.path.join(os.path.dirname(__file__), 'assets', 'clap.wav')
    
    if os.path.exists(sample_path):
        try:
            # Load the custom sample
            clap = AudioSegment.from_wav(sample_path)
            
            # If the sample is longer than the beat duration, we might want to trim it, 
            # but usually for a clap sample we want the full tail unless it's huge.
            # Let's just ensure it's not excessively long (e.g. > 1 sec)
            if len(clap) > 1000:
                clap = clap[:1000].fade_out(50)
                
            return clap
        except Exception as e:
            print(f"Error loading custom clap sample: {e}. Falling back to synthesis.")
    
    # Fallback: White noise with exponential decay
    noise = WhiteNoise().to_audio_segment(duration=duration_ms)
    # Simple envelope
    # We want a sharp attack and fast decay
    # Pydub fade_out is linear. We can simulate exp decay by multiple fades or just linear short.
    clap = noise.fade_out(duration_ms - 10)
    # High pass filter to make it crisp (approximate)
    clap = clap.high_pass_filter(800)
    return clap

def generate_fx_hit(duration_ms=1000):
    # A "Boom" or "Impact"
    # Sine wave sweep 100Hz -> 40Hz
    # Pydub doesn't have sweeps easily.
    # Let's just make a low sine ping + noise burst
    
    sine = Sine(60).to_audio_segment(duration=duration_ms).fade_out(duration_ms)
    noise = WhiteNoise().to_audio_segment(duration=duration_ms//2).fade_out(duration_ms//2).low_pass_filter(500)
    
    fx = sine.overlay(noise)
    return fx

def create_clap_loop(bpm, beats=16):
    beat_ms = 60000 / bpm
    total_duration = beats * beat_ms
    
    clap_sample = generate_clap(duration_ms=min(200, int(beat_ms/2)))
    
    # Create silence container
    loop = AudioSegment.silent(duration=int(total_duration))
    
    # Place claps on beats 2 and 4 (indices 1 and 3 in 0-indexed loop)
    # The loop repeats every 4 beats (1 measure)
    # 16 beats = 4 measures.
    # We iterate through all beats. If beat_index % 4 is 1 or 3 (2nd or 4th beat), add clap.
    
    for i in range(beats):
        # 0-indexed: 0=Beat1, 1=Beat2, 2=Beat3, 3=Beat4
        if (i % 4) == 1 or (i % 4) == 3:
            pos = int(i * beat_ms)
            loop = loop.overlay(clap_sample, position=pos)
        
    return loop

def process_track(vocals_path, inst_path, original_path, bpm):
    """
    PROCÉDURE REPRODUCTIBLE - CLAP IN EDIT
    
    Structure imposée (NON NÉGOCIABLE):
    1. Intro DJ : 16 temps (4 mesures) - Instrumental seul, PAS de clap
    2. Clap In : 16 temps (4 mesures) - Instrumental + Claps sur 2 et 4 UNIQUEMENT
    3. Body : Morceau original complet (à partir du temps 33)
    4. Outro DJ : 32 temps (8 mesures) - Instrumental seul
    
    Contraintes:
    - BPM identique à l'original (pas de time-stretch)
    - Claps courts, punchys, club-ready
    - Pas de modification mélodique
    - Cuts propres sur la grille rythmique
    """
    
    vocals = AudioSegment.from_mp3(vocals_path)
    inst = AudioSegment.from_mp3(inst_path)
    original = AudioSegment.from_mp3(original_path)
    
    beat_ms = 60000 / bpm
    ms_32_beats = 32 * beat_ms
    ms_16_beats = 16 * beat_ms
    
    edits = []
    
    # ========================================
    # CLAP IN EDIT - PROCÉDURE PAS À PAS (V4 - CLEAN)
    # ========================================
    
    # CONTRAINTE: Claps SEULS au début (pas d'instrumental pour éviter doublage)
    # Le morceau se lance APRÈS le 4ème clap.
    
    # ÉTAPE 1: Créer 8 temps de Claps seuls (4 claps sur temps 2, 4, 6, 8)
    clap_intro_beats = 8
    clap_intro = create_clap_loop(bpm, beats=clap_intro_beats)
    
    # ÉTAPE 2: Prendre le morceau original à partir du temps 9
    # (Original temps 0 = notre temps 9)
    # Donc on ne coupe rien, on prend original dès le début
    body = original
    
    # ÉTAPE 3: Outro instrumental (32 temps)
    outro_inst_32b = inst[:ms_32_beats]
    
    # ÉTAPE 4: Assembler
    # [8 temps Claps seuls] + [Morceau Original complet]
    # On REMPLACE les 32 derniers temps du morceau par l'Outro Instrumental
    # Pour avoir une fin "mixable" sans vocals
    
    # Couper les 32 derniers temps de l'original
    original_body = original[:-int(ms_32_beats)] if len(original) > ms_32_beats else original
    
    # Assembler
    clap_in_edit = clap_intro + original_body + outro_inst_32b
    
    edits.append(("Clap In", clap_in_edit))
    
    # ========================================
    # AUTRES EDITS (Versions Courtes)
    # ========================================
    
    # Blocks communs
    intro_inst_16b = inst[:ms_16_beats]
    outro_inst_32b = inst[:ms_32_beats]
    clap_loop_16 = create_clap_loop(bpm, beats=16)
    clap_in_section = intro_inst_16b.overlay(clap_loop_16)
    
    # Find Drop for vocal extraction (pour les autres versions qui en ont besoin)
    drop_start = find_drop_start(inst, beat_ms)
    drop_voc = vocals[drop_start : drop_start + ms_32_beats]
    drop_inst = inst[drop_start : drop_start + ms_32_beats]
    
    fx_hit = generate_fx_hit()
    
    # 2. Acap In
    acap_intro = drop_voc[:ms_16_beats]
    acap_in = acap_intro + original[ms_16_beats:] + outro_inst_32b
    edits.append(("Acap In", acap_in))
    
    # 3. Acap Out
    acap_out_edit = original + drop_voc
    edits.append(("Acap Out", acap_out_edit))

    # 4. Intro (Instrumental Intro)
    intro_edit = intro_inst_16b + intro_inst_16b + original[ms_32_beats:] + outro_inst_32b
    edits.append(("Intro", intro_edit))
    
    # 5. Short
    break_start = max(0, drop_start - ms_16_beats)
    break_segment = original[break_start : drop_start]
    intro_short = inst[:ms_16_beats]
    
    short_edit = intro_short + break_segment + original[drop_start : drop_start + ms_32_beats] + outro_inst_32b
    edits.append(("Short", short_edit))
    
    # 6. Short Acap In
    short_acap_in = acap_intro + break_segment + original[drop_start : drop_start + ms_32_beats] + outro_inst_32b
    edits.append(("Short Acap In", short_acap_in))
    
    # 7. Short Clap In
    short_clap_in = clap_in_section + break_segment + original[drop_start : drop_start + ms_32_beats] + outro_inst_32b
    edits.append(("Short Clap In", short_clap_in))
    
    # 8. Acap In / Acap Out
    acap_in_out = acap_intro + original + drop_voc
    edits.append(("Acap In Acap Out", acap_in_out))
    
    # 9. Slam
    slam_edit = fx_hit + original[drop_start:] + outro_inst_32b
    edits.append(("Slam", slam_edit))
    
    # 10. Short Acap Out
    short_acap_out = break_segment + original[drop_start : drop_start + ms_32_beats] + drop_voc
    edits.append(("Short Acap Out", short_acap_out))
    
    return edits


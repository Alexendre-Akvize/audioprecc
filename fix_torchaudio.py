"""
One-time fix for torchaudio torchcodec issue.
Run this ONCE on the server: python3 fix_torchaudio.py

This patches torchaudio's __init__.py to fall back to soundfile
when torchcodec is not available, instead of crashing.
"""
import os
import sys

def fix_torchaudio():
    try:
        import torchaudio
        init_path = torchaudio.__file__  # __init__.py path
        
        print(f"torchaudio location: {init_path}")
        print(f"torchaudio version: {torchaudio.__version__}")
        
        with open(init_path, 'r') as f:
            content = f.read()
        
        # Check if already patched
        if 'PATCHED_BY_IDBYRIVOLI' in content:
            print("Already patched!")
            return
        
        # Check if it has the problematic load_with_torchcodec call
        if 'load_with_torchcodec' not in content:
            print("This version of torchaudio doesn't use torchcodec. No patch needed.")
            return
        
        # Patch: wrap the load_with_torchcodec call in a try/except
        old_code = 'return load_with_torchcodec('
        new_code = '''# PATCHED_BY_IDBYRIVOLI: Fall back to soundfile if torchcodec missing
    try:
        return load_with_torchcodec('''
        
        if old_code not in content:
            print(f"Could not find expected code pattern. Manual fix needed.")
            print(f"File: {init_path}")
            return
        
        # We need to find the full return statement and wrap it
        # The pattern is:
        #     return load_with_torchcodec(
        #         uri, ...params...
        #     )
        # We'll patch the _torchcodec.py file instead - it's simpler
        
        torchcodec_path = os.path.join(os.path.dirname(init_path), '_torchcodec.py')
        if os.path.exists(torchcodec_path):
            with open(torchcodec_path, 'r') as f:
                tc_content = f.read()
            
            if 'PATCHED_BY_IDBYRIVOLI' in tc_content:
                print("_torchcodec.py already patched!")
                return
            
            # The error is raised at:
            #     raise ImportError("TorchCodec is required...")
            # We want to make the whole function fall back to soundfile
            
            old_import_error = '''raise ImportError(
        "TorchCodec is required for load_with_torchcodec. Please install torchcodec to use this function."
    )'''
            
            new_import_error = '''# PATCHED_BY_IDBYRIVOLI: Fall back to soundfile instead of crashing
        import soundfile as sf
        import torch as _torch
        data, sample_rate = sf.read(str(uri), dtype="float32")
        if data.ndim == 1:
            tensor = _torch.from_numpy(data).unsqueeze(0)
        else:
            tensor = _torch.from_numpy(data.T)
        return tensor, sample_rate'''
            
            if old_import_error in tc_content:
                tc_content = tc_content.replace(old_import_error, new_import_error)
                with open(torchcodec_path, 'w') as f:
                    f.write(tc_content)
                print(f"✅ Patched {torchcodec_path}")
                print("   torchaudio will now use soundfile as fallback when torchcodec is missing")
                return
            else:
                print(f"Could not find exact error pattern in {torchcodec_path}")
                print("Trying alternative patch...")
                
                # Try alternative pattern (different formatting)
                if 'raise ImportError' in tc_content and 'torchcodec' in tc_content.lower():
                    # Find and replace the raise ImportError line
                    lines = tc_content.split('\n')
                    new_lines = []
                    i = 0
                    patched = False
                    while i < len(lines):
                        line = lines[i]
                        if 'raise ImportError' in line and not patched:
                            # Find the full raise statement (may span multiple lines)
                            indent = len(line) - len(line.lstrip())
                            spaces = ' ' * indent
                            
                            # Skip the raise statement (may be multi-line)
                            while i < len(lines) and (lines[i].strip().endswith('(') or 
                                                       lines[i].strip().startswith('"') or
                                                       lines[i].strip().startswith("'") or
                                                       'raise ImportError' in lines[i]):
                                i += 1
                                if i < len(lines) and lines[i].strip() == ')':
                                    i += 1
                                    break
                            
                            # Insert soundfile fallback
                            new_lines.append(f'{spaces}# PATCHED_BY_IDBYRIVOLI: Fall back to soundfile')
                            new_lines.append(f'{spaces}import soundfile as sf')
                            new_lines.append(f'{spaces}import torch as _torch')
                            new_lines.append(f'{spaces}data, sample_rate = sf.read(str(uri), dtype="float32")')
                            new_lines.append(f'{spaces}if data.ndim == 1:')
                            new_lines.append(f'{spaces}    tensor = _torch.from_numpy(data).unsqueeze(0)')
                            new_lines.append(f'{spaces}else:')
                            new_lines.append(f'{spaces}    tensor = _torch.from_numpy(data.T)')
                            new_lines.append(f'{spaces}return tensor, sample_rate')
                            patched = True
                        else:
                            new_lines.append(line)
                            i += 1
                    
                    if patched:
                        with open(torchcodec_path, 'w') as f:
                            f.write('\n'.join(new_lines))
                        print(f"✅ Patched {torchcodec_path} (alternative method)")
                        return
                
                print("❌ Could not auto-patch. Please run: pip install torchcodec")
        else:
            print(f"_torchcodec.py not found at {torchcodec_path}")
            print("Please run: pip install torchcodec")
            
    except ImportError:
        print("torchaudio is not installed!")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    fix_torchaudio()
